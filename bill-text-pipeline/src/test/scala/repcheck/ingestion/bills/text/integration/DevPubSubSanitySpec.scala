package repcheck.ingestion.bills.text.integration

import java.util.UUID

import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

import cats.effect.IO
import cats.effect.unsafe.implicits.global

import io.circe.parser._

import org.http4s.ember.client.EmberClientBuilder

import doobie.implicits._

import com.google.auth.oauth2.{GoogleCredentials, ImpersonatedCredentials}
import com.google.cloud.pubsub.v1.{
  Publisher,
  SubscriptionAdminClient,
  SubscriptionAdminSettings,
  TopicAdminClient,
  TopicAdminSettings,
}
import com.google.pubsub.v1.{AcknowledgeRequest, PubsubMessage, PullRequest, SubscriptionName, TopicName}

import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock._
import com.github.tomakehurst.wiremock.core.WireMockConfiguration
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import repcheck.ingestion.bills.common.persistence.{DoobieBillRepository, DoobieBillTextVersionRepository}
import repcheck.ingestion.bills.common.testing.{E2ETest, TransactorFixture}
import repcheck.ingestion.bills.text.download.BillTextDownloader
import repcheck.ingestion.bills.text.embedding.CrossBillEmbedder
import repcheck.ingestion.bills.text.persistence.DoobieRawBillTextRepository
import repcheck.ingestion.bills.text.pipeline.BillTextProcessor
import repcheck.ingestion.bills.textcheck.api.BillTextApiClient
import repcheck.ingestion.bills.textcheck.config.BillTextCheckerConfig
import repcheck.ingestion.bills.textcheck.pipeline.BillTextAvailabilityChecker
import repcheck.ingestion.common.api.CongressGovClientConfig
import repcheck.ingestion.common.events.{DefaultIngestionEventPublisher, GooglePubSubEventPublisher}
import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}
import repcheck.ingestion.text.embedding.{EmbeddingConfig, OllamaEmbeddingService}
import repcheck.pipeline.models.events.BillTextAvailableEvent
import repcheck.shared.models.congress.bill.TextVersionCode
import repcheck.shared.models.congress.common.{BillType, Chamber}
import repcheck.shared.models.congress.dos.bill.BillDO

import com.repcheck.utils.errors.{RetryConfig, RetryWrapper}

/**
 * Hybrid E2E tests: real GCP Pub/Sub (dev project) + Docker AlloyDB Omni + WireMock. Validates the full pipeline flow:
 * seed bill in DB → checker finds it → publishes BillTextAvailableEvent to real GCP Pub/Sub → processor downloads text
 * (via WireMock) → stores in DB with embedding → validates pgvector similarity search.
 *
 * Uses service account impersonation: the caller's ADC impersonates `integration-test@repcheck-dev` to ensure tests run
 * with scoped permissions.
 *
 * Requirements: - Valid GCP credentials with `roles/iam.serviceAccountTokenCreator` on integration-test SA - Docker
 * running (for AlloyDB Omni container)
 *
 * Run with: `sbt "testOnly -- -n com.repcheck.tags.E2ETest"`
 */
class DevPubSubSanitySpec extends AnyFlatSpec with Matchers with TransactorFixture with BeforeAndAfterEach {

  private val billRepo        = new DoobieBillRepository()
  private val textVersionRepo = new DoobieBillTextVersionRepository()
  private val rawTextRepo     = new DoobieRawBillTextRepository()

  private val projectId = sys.env.getOrElse("GOOGLE_CLOUD_PROJECT", "repcheck-dev")

  private val serviceAccountEmail =
    sys.env.getOrElse("INTEGRATION_TEST_SA", s"repcheck-inttest-dev@$projectId.iam.gserviceaccount.com")

  // Ephemeral GCP Pub/Sub topic/subscription per test run
  private val testPrefix     = s"e2e-sanity-${UUID.randomUUID().toString.take(8)}"
  private val testTopicId    = s"$testPrefix-topic"
  private val testSubId      = s"$testPrefix-sub"
  private lazy val topicName = TopicName.of(projectId, testTopicId)
  private lazy val subName   = SubscriptionName.of(projectId, testSubId)

  private val wireMock = new WireMockServer(
    WireMockConfiguration
      .options()
      .bindAddress("127.0.0.1")
      .dynamicPort()
  )

  private val testRetryConfig =
    RetryConfig(maxRetries = 1, initialBackoffMs = 1L, maxBackoffMs = 10L, backoffMultiplier = 1.0)

  private lazy val (httpClient, httpShutdown) = EmberClientBuilder
    .default[IO]
    .withTimeout(10.seconds)
    .build
    .allocated
    .unsafeRunSync()

  private val testLogger = new PipelineLogger[IO] {
    override def info(context: LogContext, message: String): IO[Unit]                            = IO.unit
    override def warn(context: LogContext, message: String): IO[Unit]                            = IO.unit
    override def error(context: LogContext, message: String, cause: Option[Throwable]): IO[Unit] = IO.unit
    override def debug(context: LogContext, message: String): IO[Unit]                           = IO.unit
  }

  /** Builds impersonated credentials scoped to Pub/Sub. */
  private def impersonatedCredentials(): ImpersonatedCredentials = {
    val sourceCredentials = GoogleCredentials.getApplicationDefault()
    ImpersonatedCredentials
      .newBuilder()
      .setSourceCredentials(sourceCredentials)
      .setTargetPrincipal(serviceAccountEmail)
      .setScopes(java.util.Arrays.asList("https://www.googleapis.com/auth/pubsub"))
      .setLifetime(300)
      .build()
  }

  /** Immutable bundle of GCP Pub/Sub resources, created lazily once per suite. */
  final private case class GcpResources(
    topicAdmin: TopicAdminClient,
    subAdmin: SubscriptionAdminClient,
    publisher: Publisher,
    subscriberStub: com.google.cloud.pubsub.v1.stub.GrpcSubscriberStub,
  )

  /** Lazily initializes GCP resources. Returns None if credentials are unavailable. */
  private lazy val gcpResources: Option[GcpResources] =
    try {
      val creds = impersonatedCredentials()

      val topicAdmin =
        TopicAdminClient.create(TopicAdminSettings.newBuilder().setCredentialsProvider(() => creds).build())
      val subAdmin =
        SubscriptionAdminClient.create(
          SubscriptionAdminSettings.newBuilder().setCredentialsProvider(() => creds).build()
        )

      val _ = topicAdmin.createTopic(topicName)
      val _ = subAdmin.createSubscription(
        subName,
        topicName,
        com.google.pubsub.v1.PushConfig.getDefaultInstance,
        10,
      )

      val pub = Publisher.newBuilder(topicName).setCredentialsProvider(() => creds).build()
      val stub = com.google.cloud.pubsub.v1.stub.GrpcSubscriberStub.create(
        com.google.cloud.pubsub.v1.stub.SubscriberStubSettings
          .newBuilder()
          .setCredentialsProvider(() => creds)
          .build()
      )

      Some(GcpResources(topicAdmin, subAdmin, pub, stub))
    } catch { case _: Exception => None }

  override def beforeAll(): Unit = {
    super.beforeAll()
    wireMock.start()
    val _ = gcpResources // force lazy initialization
  }

  // Track CrossBillEmbedder finalizers allocated by `buildProcessor` so we can release them in afterAll.
  private val embedderFinalizers =
    new java.util.concurrent.ConcurrentLinkedQueue[IO[Unit]]()

  override def afterAll(): Unit = {
    wireMock.stop()
    embedderFinalizers.forEach { fin =>
      try fin.unsafeRunSync()
      catch { case _: Exception => () }
    }
    try httpShutdown.unsafeRunSync()
    catch { case _: Exception => () }
    gcpResources.foreach { r =>
      try r.publisher.shutdown()
      catch { case _: Exception => () }
      try r.subscriberStub.close()
      catch { case _: Exception => () }
      try r.subAdmin.deleteSubscription(subName)
      catch { case _: Exception => () }
      try r.topicAdmin.deleteTopic(topicName)
      catch { case _: Exception => () }
      try r.topicAdmin.close()
      catch { case _: Exception => () }
      try r.subAdmin.close()
      catch { case _: Exception => () }
    }
    super.afterAll()
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    gcpResources.foreach(_ => drainGcpMessages())
  }

  override def afterEach(): Unit = {
    wireMock.resetAll()
    super.afterEach()
  }

  /** Drains pending messages from the GCP subscription to isolate tests. */
  private def drainGcpMessages(): Unit = {
    val _ = scala.util.Try {
      gcpResources.foreach { r =>
        val pullRequest = PullRequest
          .newBuilder()
          .setSubscription(subName.toString)
          .setMaxMessages(100)
          .build()
        val response = r.subscriberStub.pullCallable().call(pullRequest)
        val messages = response.getReceivedMessagesList.asScala.toList
        if (messages.nonEmpty) {
          val ackIds = messages.map(_.getAckId).asJava
          val ackRequest = AcknowledgeRequest
            .newBuilder()
            .setSubscription(subName.toString)
            .addAllAckIds(ackIds)
            .build()
          val _ = r.subscriberStub.acknowledgeCallable().call(ackRequest)
        }
      }
    }
  }

  /** Pulls messages from the GCP subscription with auto-ack. */
  private def pullGcpMessages(maxMessages: Int = 10): List[PubsubMessage] =
    gcpResources
      .map { r =>
        val pullRequest = PullRequest
          .newBuilder()
          .setSubscription(subName.toString)
          .setMaxMessages(maxMessages)
          .build()
        val response = r.subscriberStub.pullCallable().call(pullRequest)
        val messages = response.getReceivedMessagesList.asScala.toList

        if (messages.nonEmpty) {
          val ackIds = messages.map(_.getAckId).asJava
          val ackRequest = AcknowledgeRequest
            .newBuilder()
            .setSubscription(subName.toString)
            .addAllAckIds(ackIds)
            .build()
          val _ = r.subscriberStub.acknowledgeCallable().call(ackRequest)
        }

        messages.map(_.getMessage)
      }
      .getOrElse(List.empty)

  // --- Wiring helpers ---

  private def buildChecker(): BillTextAvailabilityChecker[IO] = {
    val r            = gcpResources.getOrElse(fail("GCP resources not available"))
    val retryWrapper = new RetryWrapper[IO]((_, _, _, _, _, _) => IO.unit)
    val congressConfig = CongressGovClientConfig(
      apiKey = "test-api-key",
      baseUrl = s"http://127.0.0.1:${wireMock.port().toString}/v3",
      pageSize = 250,
      pageDelay = Duration.Zero,
      retry = testRetryConfig,
    )
    val textApiClient   = new BillTextApiClient[IO](congressConfig, httpClient, retryWrapper)
    val pubsubPublisher = new GooglePubSubEventPublisher[IO](r.publisher)
    val eventPublisher =
      new DefaultIngestionEventPublisher[IO](
        pubsubPublisher,
        topicName.toString,
        "e2e-sanity",
        new RetryWrapper[IO]((_, _, _, _, _, _) => IO.unit),
        testRetryConfig,
      )

    new BillTextAvailabilityChecker[IO](
      textApiClient = textApiClient,
      billRepo = billRepo,
      eventPublisher = eventPublisher,
      retryWrapper = retryWrapper,
      xa = xa,
      config = BillTextCheckerConfig(parallelism = 1, eventPublishRetry = testRetryConfig, congresses = ""),
      logger = testLogger,
    )
  }

  private def buildProcessorWithOllama(): BillTextProcessor[IO] = {
    val r = gcpResources.getOrElse(fail("GCP resources not available"))
    val downloader = new BillTextDownloader[IO](
      client = httpClient,
      govInfoApiKey = "integration-test-key",
      govInfoBaseUrl = "https://api.govinfo.gov",
      logger = testLogger,
    )
    val embeddingConfig = EmbeddingConfig(
      baseUrl = s"http://127.0.0.1:${wireMock.port().toString}",
      modelName = "bill-text-embedding",
      dimensions = 1024,
      timeoutSeconds = 10,
      maxChunkChars = 30000,
      embedBatchSize = 10,
      embedBatchTimeout = scala.concurrent.duration.DurationInt(1).second,
      embedQueueCapacityMultiplier = 10,
    )
    val embeddingService = new OllamaEmbeddingService[IO](httpClient, embeddingConfig, testLogger)
    val pubsubPublisher  = new GooglePubSubEventPublisher[IO](r.publisher)
    val eventPublisher =
      new DefaultIngestionEventPublisher[IO](
        pubsubPublisher,
        topicName.toString,
        "e2e-sanity",
        new RetryWrapper[IO]((_, _, _, _, _, _) => IO.unit),
        testRetryConfig,
      )

    val (embedder, fin) = CrossBillEmbedder
      .resource[IO](
        embeddingService = embeddingService,
        rawBillTextRepository = rawTextRepo,
        textVersionRepository = textVersionRepo,
        xa = xa,
        logger = testLogger,
        batchSize = embeddingConfig.embedBatchSize,
      )
      .allocated
      .unsafeRunSync()
    val _ = embedderFinalizers.offer(fin)

    new BillTextProcessor[IO](
      downloader = downloader,
      billRepository = billRepo,
      textVersionRepository = textVersionRepo,
      embedder = embedder,
      embeddingConfig = embeddingConfig,
      eventPublisher = eventPublisher,
      xa = xa,
      logger = testLogger,
      extractText = (bytes, format) =>
        repcheck.ingestion.text.extraction.TextExtractor.extractStream[IO](bytes, format),
    )
  }

  private def seedBill(naturalKey: String, number: String): Long = {
    val bill = BillDO(
      billId = 0L,
      naturalKey = naturalKey,
      congress = 118,
      billType = BillType.HR,
      number = number,
      title = "E2E Sanity Test Bill",
      originChamber = Some(Chamber.House),
      originChamberCode = Some("H"),
      introducedDate = None,
      policyArea = None,
      latestActionDate = None,
      latestActionText = None,
      constitutionalAuthorityText = None,
      sponsorMemberId = None,
      textVersionType = None,
      updateDate = None,
      updateDateIncludingText = None,
      legislationUrl = None,
      apiUrl = None,
      createdAt = None,
      updatedAt = None,
      latestTextVersionId = None,
    )
    val billId = billRepo.upsert(bill).transact(xa).unsafeRunSync()
    // Stage-aware sweep filter (PR #77) requires expected_text_version_code IS NOT NULL.
    // Bills used by these E2E sanity tests are introduced; House → IH floor.
    val _ = billRepo.updateExpectedVersion(naturalKey, TextVersionCode.IH).transact(xa).unsafeRunSync()
    billId
  }

  private val billTextHtml: String =
    """<html><body><pre>
      |SECTION 1. SHORT TITLE.
      |This Act may be cited as the "E2E Sanity Test Act".
      |
      |SECTION 2. PURPOSE.
      |To validate the complete bill text ingestion pipeline end-to-end.
      |</pre></body></html>""".stripMargin

  private def textVersionsJson(textUrl: String): String =
    s"""{
       |  "textVersions": [
       |    {
       |      "date": "2024-01-15T00:00:00Z",
       |      "type": "IH",
       |      "formats": [
       |        {"type": "Formatted Text", "url": "$textUrl"}
       |      ]
       |    }
       |  ]
       |}""".stripMargin

  private def stubTextDownload(path: String, htmlContent: String): Unit = {
    val _ = wireMock.stubFor(
      get(urlPathEqualTo(path))
        .willReturn(
          aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "text/html")
            .withBody(htmlContent)
        )
    )
  }

  private def stubOllamaEmbedding(embedding: Array[Float]): Unit = {
    val embeddingJson = embedding.map(_.toString).mkString("[", ",", "]")
    val _ = wireMock.stubFor(
      post(urlEqualTo("/api/embed"))
        .willReturn(
          aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/json")
            .withBody(s"""{"embeddings":[$embeddingJson]}""")
        )
    )
  }

  private def extractPayloadField(messageData: String, field: String): Option[String] =
    parse(messageData).toOption
      .flatMap(_.hcursor.downField("payload").downField(field).as[String].toOption)

  private def parseEvent(msg: PubsubMessage): BillTextAvailableEvent = {
    val data    = msg.getData.toStringUtf8
    val json    = parse(data).getOrElse(fail("Failed to parse event JSON"))
    val payload = json.hcursor.downField("payload")
    BillTextAvailableEvent(
      naturalKey = payload.downField("naturalKey").as[String].getOrElse(""),
      congress = payload.downField("congress").as[Int].getOrElse(0),
      textUrl = payload.downField("textUrl").as[String].getOrElse(""),
      textFormat = payload.downField("textFormat").as[String].getOrElse(""),
      versionCode = payload.downField("versionCode").as[String].getOrElse(""),
      previousVersionCode = payload.downField("previousVersionCode").as[String].toOption,
    )
  }

  // --- E2E Tests ---

  "Dev E2E" should "run checker → GCP Pub/Sub → processor → DB" taggedAs E2ETest in {
    val _ = assume(gcpResources.isDefined, "GCP credentials not available — skipping")

    val dbBillId = seedBill("118-HR-700", "700")
    val textUrl  = s"http://127.0.0.1:${wireMock.port().toString}/text/118/hr/700/ih"

    // Stub Congress.gov text versions API
    val _ = wireMock.stubFor(
      get(urlPathEqualTo("/v3/bill/118/hr/700/text"))
        .willReturn(
          aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/json")
            .withBody(textVersionsJson(textUrl))
        )
    )
    stubTextDownload("/text/118/hr/700/ih", billTextHtml)
    stubOllamaEmbedding(Array.fill(1024)(0.0f).updated(0, 1.0f))

    // Step 1: Checker finds bill, publishes to real GCP Pub/Sub
    val checker        = buildChecker()
    val checkerResults = checker.checkAll(0L).compile.toList.unsafeRunSync()
    val _              = checkerResults.size shouldBe 1
    val _              = checkerResults.headOption.exists(_.isSucceeded) shouldBe true

    // Step 2: Pull event from real GCP Pub/Sub
    Thread.sleep(2000L)
    val checkerMessages = pullGcpMessages()
    val _               = checkerMessages should not be empty
    val event           = parseEvent(checkerMessages.headOption.getOrElse(fail("No message")))

    // Step 3: Processor downloads text, stores in DB with embedding
    val processor = buildProcessorWithOllama()
    val result = processor
      .processEvent(event, UUID.randomUUID(), s"ack-${UUID.randomUUID().toString}", IO.unit, IO.unit)
      .unsafeRunSync()
    val _ = result.isSucceeded shouldBe true

    // Step 4: Verify text stored in DB
    val versions = textVersionRepo.findByBillId(dbBillId).transact(xa).unsafeRunSync()
    val _        = versions.size shouldBe 1
    val stored   = versions.headOption.getOrElse(fail("No version stored"))
    val _        = stored.versionCode shouldBe "IH"
    val _        = stored.billId shouldBe dbBillId
    val chunks   = rawTextRepo.findByVersionId(stored.id).transact(xa).unsafeRunSync()
    chunks.map(_.content).mkString should include("E2E Sanity Test Act")
  }

  it should "store embedding and find bill via vector search" taggedAs E2ETest in {
    val _ = assume(gcpResources.isDefined, "GCP credentials not available — skipping")

    val dbBillId = seedBill("118-HR-701", "701")
    val textUrl  = s"http://127.0.0.1:${wireMock.port().toString}/text/118/hr/701/ih"

    val knownEmbedding = Array.fill(1024)(0.0f).updated(0, 1.0f)

    val _ = wireMock.stubFor(
      get(urlPathEqualTo("/v3/bill/118/hr/701/text"))
        .willReturn(
          aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/json")
            .withBody(textVersionsJson(textUrl))
        )
    )
    stubTextDownload("/text/118/hr/701/ih", billTextHtml)
    stubOllamaEmbedding(knownEmbedding)

    // Run full chain: checker → GCP Pub/Sub → processor → DB
    val checker = buildChecker()
    val _       = checker.checkAll(0L).compile.toList.unsafeRunSync()
    Thread.sleep(2000L)
    val messages = pullGcpMessages()
    val event    = parseEvent(messages.headOption.getOrElse(fail("No message")))

    val processor = buildProcessorWithOllama()
    val _ = processor
      .processEvent(event, UUID.randomUUID(), s"ack-${UUID.randomUUID().toString}", IO.unit, IO.unit)
      .unsafeRunSync()

    // Verify embedding stored on at least one chunk
    val versions   = textVersionRepo.findByBillId(dbBillId).transact(xa).unsafeRunSync()
    val storedV    = versions.headOption.getOrElse(fail("No version stored"))
    val storedRows = rawTextRepo.findByVersionId(storedV.id).transact(xa).unsafeRunSync()
    val _          = storedRows.exists(_.embedding.isDefined) shouldBe true

    // Verify pgvector cosine similarity search against raw_bill_text
    val queryVector = knownEmbedding.mkString("[", ",", "]")
    val similarity = sql"""
      SELECT 1 - (embedding <=> $queryVector::vector) as similarity
      FROM raw_bill_text
      WHERE bill_id = $dbBillId AND embedding IS NOT NULL
      ORDER BY embedding <=> $queryVector::vector
      LIMIT 1
    """.query[Double].option.transact(xa).unsafeRunSync()

    val _ = similarity.isDefined shouldBe true
    similarity.getOrElse(0.0) should be > 0.99
  }

  it should "find similar bills via cross-bill vector search" taggedAs E2ETest in {
    val _ = assume(gcpResources.isDefined, "GCP credentials not available — skipping")

    val billId1 = seedBill("118-HR-702", "702")
    val _       = seedBill("118-HR-703", "703")

    // Bill 702: unit vector in dimension 0
    val embedding1 = Array.fill(1024)(0.0f).updated(0, 1.0f)
    // Bill 703: unit vector in dimension 1 (orthogonal)
    val embedding2 = Array.fill(1024)(0.0f).updated(1, 1.0f)

    val textUrl1 = s"http://127.0.0.1:${wireMock.port().toString}/text/118/hr/702/ih"
    val textUrl2 = s"http://127.0.0.1:${wireMock.port().toString}/text/118/hr/703/ih"

    // Stub Congress.gov API for both bills
    val _ = wireMock.stubFor(
      get(urlPathEqualTo("/v3/bill/118/hr/702/text"))
        .willReturn(
          aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/json")
            .withBody(textVersionsJson(textUrl1))
        )
    )
    val _ = wireMock.stubFor(
      get(urlPathEqualTo("/v3/bill/118/hr/703/text"))
        .willReturn(
          aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/json")
            .withBody(textVersionsJson(textUrl2))
        )
    )
    stubTextDownload("/text/118/hr/702/ih", billTextHtml)
    stubTextDownload("/text/118/hr/703/ih", billTextHtml)

    // Run checker — finds both bills
    stubOllamaEmbedding(embedding1)
    val checker = buildChecker()
    val results = checker.checkAll(0L).compile.toList.unsafeRunSync()
    val _       = results.size shouldBe 2

    // Pull messages with retry — Pub/Sub may not deliver all messages in one pull
    val (messages, attempts) = (1 to 5).foldLeft((List.empty[PubsubMessage], 0)) {
      case ((msgs, att), _) =>
        if (msgs.size >= 2) (msgs, att)
        else {
          Thread.sleep(3000L)
          (msgs ++ pullGcpMessages(10), att + 1)
        }
    }

    // Process bill 702 with embedding1
    val processor = buildProcessorWithOllama()
    val msg702 = messages
      .find(m => extractPayloadField(m.getData.toStringUtf8, "naturalKey").contains("118-HR-702"))
      .getOrElse(fail(s"No event for bill 702 after $attempts pulls (got ${messages.size} messages)"))
    val _ = processor
      .processEvent(parseEvent(msg702), UUID.randomUUID(), s"ack-${UUID.randomUUID().toString}", IO.unit, IO.unit)
      .unsafeRunSync()

    // Switch to embedding2 for bill 703
    wireMock.resetMappings()
    stubOllamaEmbedding(embedding2)
    stubTextDownload("/text/118/hr/703/ih", billTextHtml)

    val msg703 = messages
      .find(m => extractPayloadField(m.getData.toStringUtf8, "naturalKey").contains("118-HR-703"))
      .getOrElse(fail("No event for bill 703"))
    val _ = processor
      .processEvent(parseEvent(msg703), UUID.randomUUID(), s"ack-${UUID.randomUUID().toString}", IO.unit, IO.unit)
      .unsafeRunSync()

    // Verify cross-bill vector search: query with embedding1 ranks bill 702 first.
    // Embeddings now live on raw_bill_text rows (P6.H4c refactor), so the search joins via that table.
    val queryVector = embedding1.mkString("[", ",", "]")
    val searchResults = sql"""
      SELECT rbt.bill_id, 1 - (rbt.embedding <=> $queryVector::vector) as similarity
      FROM raw_bill_text rbt
      WHERE rbt.embedding IS NOT NULL
      ORDER BY rbt.embedding <=> $queryVector::vector
      LIMIT 2
    """.query[(Long, Double)].to[List].transact(xa).unsafeRunSync()

    val _ = searchResults.size shouldBe 2
    // First: bill 702 (identical vector, similarity ~1.0)
    val _ = searchResults.headOption.map(_._1).getOrElse(0L) shouldBe billId1
    val _ = searchResults.headOption.map(_._2).getOrElse(0.0) should be > 0.99
    // Second: bill 703 (orthogonal, similarity ~0.0)
    searchResults.lastOption.map(_._2).getOrElse(1.0) should be < 0.01
  }

  it should "verify dev Pub/Sub topic is accessible" taggedAs E2ETest in {
    val _ = assume(gcpResources.isDefined, "GCP credentials not available — skipping")

    val topicAdmin = TopicAdminClient.create(
      TopicAdminSettings.newBuilder().setCredentialsProvider(() => impersonatedCredentials()).build()
    )
    try {
      val topics = topicAdmin.listTopics(s"projects/$projectId").iterateAll().asScala.toList
      topics.toString should not be empty
    } finally topicAdmin.close()
  }

}
