package com.repcheck.bills.text.integration

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
import repcheck.ingestion.common.api.CongressGovClientConfig
import repcheck.ingestion.common.events.{DefaultIngestionEventPublisher, GooglePubSubEventPublisher}
import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}
import repcheck.pipeline.models.errors.{RetryConfig, RetryWrapper}
import repcheck.pipeline.models.events.BillTextAvailableEvent
import repcheck.shared.models.congress.common.{BillType, Chamber}
import repcheck.shared.models.congress.dos.bill.BillDO

import com.repcheck.bills.common.persistence.{DoobieBillRepository, DoobieBillTextVersionRepository}
import com.repcheck.bills.common.testing.{E2ETest, TransactorFixture}
import com.repcheck.bills.text.config.BillTextPipelineConfig
import com.repcheck.bills.text.download.BillTextDownloader
import com.repcheck.bills.text.embedding.{EmbeddingConfig, OllamaEmbeddingService}
import com.repcheck.bills.text.pipeline.BillTextProcessor
import com.repcheck.bills.textcheck.api.BillTextApiClient
import com.repcheck.bills.textcheck.config.BillTextCheckerConfig
import com.repcheck.bills.textcheck.pipeline.BillTextAvailabilityChecker

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

  override def afterAll(): Unit = {
    wireMock.stop()
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
      new DefaultIngestionEventPublisher[IO](pubsubPublisher, topicName.toString, "e2e-sanity")

    new BillTextAvailabilityChecker[IO](
      textApiClient = textApiClient,
      billRepo = billRepo,
      eventPublisher = eventPublisher,
      retryWrapper = retryWrapper,
      xa = xa,
      config = BillTextCheckerConfig(parallelism = 1, eventPublishRetry = testRetryConfig),
      logger = testLogger,
    )
  }

  private def buildProcessorWithOllama(): BillTextProcessor[IO] = {
    val r              = gcpResources.getOrElse(fail("GCP resources not available"))
    val pipelineConfig = BillTextPipelineConfig(1, 10, 10485760L)
    val downloader     = new BillTextDownloader[IO](httpClient, pipelineConfig, testLogger)
    val embeddingConfig = EmbeddingConfig(
      baseUrl = s"http://127.0.0.1:${wireMock.port().toString}",
      modelName = "qwen3-embedding",
      dimensions = 1536,
      timeoutSeconds = 10,
    )
    val embeddingService = new OllamaEmbeddingService[IO](httpClient, embeddingConfig, testLogger)
    val pubsubPublisher  = new GooglePubSubEventPublisher[IO](r.publisher)
    val eventPublisher =
      new DefaultIngestionEventPublisher[IO](pubsubPublisher, topicName.toString, "e2e-sanity")

    new BillTextProcessor[IO](
      downloader = downloader,
      billRepository = billRepo,
      textVersionRepository = textVersionRepo,
      embeddingService = embeddingService,
      eventPublisher = eventPublisher,
      xa = xa,
      logger = testLogger,
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
      textUrl = None,
      textFormat = None,
      textVersionType = None,
      textDate = None,
      textContent = None,
      textEmbedding = None,
      summaryText = None,
      summaryActionDesc = None,
      summaryActionDate = None,
      updateDate = None,
      updateDateIncludingText = None,
      legislationUrl = None,
      apiUrl = None,
      createdAt = None,
      updatedAt = None,
      latestTextVersionId = None,
    )
    billRepo.upsert(bill).transact(xa).unsafeRunSync()
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
    stubOllamaEmbedding(Array.fill(1536)(0.0f).updated(0, 1.0f))

    // Step 1: Checker finds bill, publishes to real GCP Pub/Sub
    val checker        = buildChecker()
    val checkerResults = checker.checkAll(UUID.randomUUID()).compile.toList.unsafeRunSync()
    val _              = checkerResults.size shouldBe 1
    val _              = checkerResults.headOption.exists(_.isSucceeded) shouldBe true

    // Step 2: Pull event from real GCP Pub/Sub
    Thread.sleep(2000L)
    val checkerMessages = pullGcpMessages()
    val _               = checkerMessages should not be empty
    val event           = parseEvent(checkerMessages.headOption.getOrElse(fail("No message")))

    // Step 3: Processor downloads text, stores in DB with embedding
    val processor = buildProcessorWithOllama()
    val result    = processor.processEvent(event, UUID.randomUUID()).unsafeRunSync()
    val _         = result.isSucceeded shouldBe true

    // Step 4: Verify text stored in DB
    val versions = textVersionRepo.findByBillId(dbBillId).transact(xa).unsafeRunSync()
    val _        = versions.size shouldBe 1
    val stored   = versions.headOption.getOrElse(fail("No version stored"))
    val _        = stored.versionCode shouldBe "IH"
    val _        = stored.billId shouldBe dbBillId
    stored.content.getOrElse("") should include("E2E Sanity Test Act")
  }

  it should "store embedding and find bill via vector search" taggedAs E2ETest in {
    val _ = assume(gcpResources.isDefined, "GCP credentials not available — skipping")

    val dbBillId = seedBill("118-HR-701", "701")
    val textUrl  = s"http://127.0.0.1:${wireMock.port().toString}/text/118/hr/701/ih"

    val knownEmbedding = Array.fill(1536)(0.0f).updated(0, 1.0f)

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
    val _       = checker.checkAll(UUID.randomUUID()).compile.toList.unsafeRunSync()
    Thread.sleep(2000L)
    val messages = pullGcpMessages()
    val event    = parseEvent(messages.headOption.getOrElse(fail("No message")))

    val processor = buildProcessorWithOllama()
    val _         = processor.processEvent(event, UUID.randomUUID()).unsafeRunSync()

    // Verify embedding stored
    val versions = textVersionRepo.findByBillId(dbBillId).transact(xa).unsafeRunSync()
    val _        = versions.headOption.flatMap(_.embedding).isDefined shouldBe true

    // Verify pgvector cosine similarity search
    val queryVector = knownEmbedding.mkString("[", ",", "]")
    val similarity = sql"""
      SELECT 1 - (embedding <=> $queryVector::vector) as similarity
      FROM bill_text_versions
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
    val embedding1 = Array.fill(1536)(0.0f).updated(0, 1.0f)
    // Bill 703: unit vector in dimension 1 (orthogonal)
    val embedding2 = Array.fill(1536)(0.0f).updated(1, 1.0f)

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
    val results = checker.checkAll(UUID.randomUUID()).compile.toList.unsafeRunSync()
    val _       = results.size shouldBe 2

    // Pull messages with retry — Pub/Sub may not deliver all messages in one pull
    var messages = List.empty[PubsubMessage]
    var attempts = 0
    while (messages.size < 2 && attempts < 5) {
      Thread.sleep(3000L)
      messages = messages ++ pullGcpMessages(10)
      attempts += 1
    }

    // Process bill 702 with embedding1
    val processor = buildProcessorWithOllama()
    val msg702 = messages
      .find(m => extractPayloadField(m.getData.toStringUtf8, "naturalKey").contains("118-HR-702"))
      .getOrElse(fail(s"No event for bill 702 after $attempts pulls (got ${messages.size} messages)"))
    val _ = processor.processEvent(parseEvent(msg702), UUID.randomUUID()).unsafeRunSync()

    // Switch to embedding2 for bill 703
    wireMock.resetMappings()
    stubOllamaEmbedding(embedding2)
    stubTextDownload("/text/118/hr/703/ih", billTextHtml)

    val msg703 = messages
      .find(m => extractPayloadField(m.getData.toStringUtf8, "naturalKey").contains("118-HR-703"))
      .getOrElse(fail("No event for bill 703"))
    val _ = processor.processEvent(parseEvent(msg703), UUID.randomUUID()).unsafeRunSync()

    // Verify cross-bill vector search: query with embedding1 ranks bill 702 first
    val queryVector = embedding1.mkString("[", ",", "]")
    val searchResults = sql"""
      SELECT btv.bill_id, 1 - (btv.embedding <=> $queryVector::vector) as similarity
      FROM bill_text_versions btv
      WHERE btv.embedding IS NOT NULL
      ORDER BY btv.embedding <=> $queryVector::vector
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
