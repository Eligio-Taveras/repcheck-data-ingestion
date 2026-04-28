package repcheck.ingestion.bills.text.integration

import java.util.UUID

import scala.concurrent.duration._

import cats.effect.IO
import cats.effect.unsafe.implicits.global

import io.circe.parser._

import org.http4s.ember.client.EmberClientBuilder

import doobie.implicits._

import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock._
import com.github.tomakehurst.wiremock.core.WireMockConfiguration
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import repcheck.ingestion.bills.common.persistence.{
  DoobieBillCosponsorRepository,
  DoobieBillHistoryArchiver,
  DoobieBillRepository,
  DoobieBillSubjectRepository,
  DoobieBillTextVersionRepository,
}
import repcheck.ingestion.bills.common.testing.{DockerRequired, PubSubEmulatorFixture, TransactorFixture}
import repcheck.ingestion.bills.metadata.api.BillsApiClient
import repcheck.ingestion.bills.metadata.config.BillMetadataConfig
import repcheck.ingestion.bills.metadata.pipeline.BillMetadataProcessor
import repcheck.ingestion.bills.text.download.BillTextDownloader
import repcheck.ingestion.bills.text.embedding.{
  CrossBillEmbedder,
  EmbeddingConfig,
  NoOpEmbeddingService,
  OllamaEmbeddingService,
}
import repcheck.ingestion.bills.text.persistence.DoobieRawBillTextRepository
import repcheck.ingestion.bills.text.pipeline.BillTextProcessor
import repcheck.ingestion.bills.textcheck.api.BillTextApiClient
import repcheck.ingestion.bills.textcheck.config.BillTextCheckerConfig
import repcheck.ingestion.bills.textcheck.pipeline.BillTextAvailabilityChecker
import repcheck.ingestion.common.api.CongressGovClientConfig
import repcheck.ingestion.common.events.{DefaultIngestionEventPublisher, GooglePubSubEventPublisher}
import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}
import repcheck.ingestion.common.placeholders.{EntityRepository, PlaceholderCreator}
import repcheck.members.common.persistence.DoobieMemberRepository
import repcheck.pipeline.models.errors.{RetryConfig, RetryWrapper}
import repcheck.pipeline.models.events.BillTextAvailableEvent
import repcheck.shared.models.congress.dos.member.MemberDO
import repcheck.shared.models.placeholder.HasPlaceholder

/**
 * Full lifecycle integration test: metadata ingestion → DB storage → text availability check → Pub/Sub event → text
 * download → embedding → DB storage → pgvector similarity search.
 *
 * This test exercises the complete bill ingestion pipeline end-to-end: 1. BillMetadataProcessor fetches bill metadata
 * from Congress.gov (WireMock) and stores it in AlloyDB Omni (Docker) 2. BillTextAvailabilityChecker finds bills
 * needing text and publishes BillTextAvailableEvent to Pub/Sub (emulator) 3. BillTextProcessor downloads text
 * (WireMock), generates embedding (WireMock Ollama), stores in DB 4. Test validates pgvector cosine similarity search
 * finds the bill
 *
 * Uses: Docker AlloyDB Omni + Docker Pub/Sub emulator + WireMock for all HTTP endpoints
 */
class MetadataToVectorSearchLifecycleSpec
    extends AnyFlatSpec
    with Matchers
    with TransactorFixture
    with PubSubEmulatorFixture
    with BeforeAndAfterEach {

  private val wireMock = new WireMockServer(
    WireMockConfiguration
      .options()
      .bindAddress("127.0.0.1")
      .dynamicPort()
  )

  // Real Doobie repositories backed by Docker AlloyDB Omni
  private val billRepo        = new DoobieBillRepository()
  private val textVersionRepo = new DoobieBillTextVersionRepository()
  private val rawTextRepo     = new DoobieRawBillTextRepository()

  private val noEmbedConfig: EmbeddingConfig = EmbeddingConfig(
    baseUrl = "http://127.0.0.1:0",
    modelName = "bill-text-embedding",
    dimensions = 1024,
    timeoutSeconds = 10,
    maxChunkChars = 30000,
    embedBatchSize = 10,
    embedBatchTimeout = scala.concurrent.duration.DurationInt(1).second,
    embedQueueCapacityMultiplier = 10,
  )

  private val cosponsorRepo   = new DoobieBillCosponsorRepository()
  private val subjectRepo     = new DoobieBillSubjectRepository()
  private val historyArchiver = new DoobieBillHistoryArchiver()
  private val memberRepo      = new DoobieMemberRepository()

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

  /**
   * No-op PlaceholderCreator. Members are pre-seeded by TransactorFixture so we don't need real placeholder creation.
   * The WireMock responses use bioguide IDs matching pre-seeded members.
   */
  private val stubPlaceholderCreator: PlaceholderCreator[IO] = new PlaceholderCreator[IO] {
    def ensureExists[T <: Product](
      naturalKey: String,
      repository: EntityRepository[IO, T],
    )(using HasPlaceholder[T]): IO[Unit] = IO.unit
  }

  /** No-op EntityRepository for members — placeholders are pre-seeded. */
  private val stubMemberEntityRepo: EntityRepository[IO, MemberDO] = new EntityRepository[IO, MemberDO] {
    override def insertIfNotExists(entity: MemberDO): IO[Unit] = IO.unit
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    wireMock.start()
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
    super.afterAll()
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    drainMessages()
  }

  override def afterEach(): Unit = {
    wireMock.resetAll()
    super.afterEach()
  }

  private def wmBaseUrl: String = s"http://127.0.0.1:${wireMock.port().toString}"

  // --- Wiring helpers ---

  private def buildMetadataProcessor(): BillMetadataProcessor[IO] = {
    val retryWrapper = new RetryWrapper[IO]((_, _, _, _, _, _) => IO.unit)
    val congressConfig = CongressGovClientConfig(
      apiKey = "test-api-key",
      baseUrl = s"$wmBaseUrl/v3",
      pageSize = 250,
      pageDelay = Duration.Zero,
      retry = testRetryConfig,
    )
    val apiClient = BillsApiClient[IO](congressConfig, httpClient, retryWrapper)

    new BillMetadataProcessor[IO](
      apiClient = apiClient,
      billRepo = billRepo,
      cosponsorRepo = cosponsorRepo,
      subjectRepo = subjectRepo,
      historyArchiver = historyArchiver,
      memberRepo = memberRepo,
      placeholderCreator = stubPlaceholderCreator,
      memberEntityRepo = stubMemberEntityRepo,
      xa = xa,
      config = BillMetadataConfig(lookbackDays = 30, parallelism = 1),
      logger = testLogger,
    )
  }

  private def buildChecker(): BillTextAvailabilityChecker[IO] = {
    val retryWrapper = new RetryWrapper[IO]((_, _, _, _, _, _) => IO.unit)
    val congressConfig = CongressGovClientConfig(
      apiKey = "test-api-key",
      baseUrl = s"$wmBaseUrl/v3",
      pageSize = 250,
      pageDelay = Duration.Zero,
      retry = testRetryConfig,
    )
    val textApiClient   = new BillTextApiClient[IO](congressConfig, httpClient, retryWrapper)
    val pubsubPublisher = new GooglePubSubEventPublisher[IO](publisher)
    val eventPublisher =
      new DefaultIngestionEventPublisher[IO](
        pubsubPublisher,
        topicName.toString,
        "lifecycle-test",
        new RetryWrapper[IO]((_, _, _, _, _, _) => IO.unit),
        testRetryConfig,
      )

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

  private def buildProcessor(withEmbedding: Boolean): BillTextProcessor[IO] = {
    val downloader = new BillTextDownloader[IO](
      client = httpClient,
      govInfoApiKey = "integration-test-key",
      govInfoBaseUrl = "https://api.govinfo.gov",
      logger = testLogger,
    )
    val (embeddingService, embeddingConfig) =
      if (withEmbedding) {
        val cfg = EmbeddingConfig(
          baseUrl = wmBaseUrl,
          modelName = "bill-text-embedding",
          dimensions = 1024,
          timeoutSeconds = 10,
          maxChunkChars = 30000,
          embedBatchSize = 10,
          embedBatchTimeout = scala.concurrent.duration.DurationInt(1).second,
          embedQueueCapacityMultiplier = 10,
        )
        (new OllamaEmbeddingService[IO](httpClient, cfg, testLogger), cfg)
      } else {
        (new NoOpEmbeddingService[IO], noEmbedConfig)
      }
    val pubsubPublisher = new GooglePubSubEventPublisher[IO](publisher)
    val eventPublisher =
      new DefaultIngestionEventPublisher[IO](
        pubsubPublisher,
        topicName.toString,
        "lifecycle-test",
        new RetryWrapper[IO]((_, _, _, _, _, _) => IO.unit),
        testRetryConfig,
      )

    val (embedder, fin) = CrossBillEmbedder
      .resource[IO](
        embeddingService = embeddingService,
        rawBillTextRepository = rawTextRepo,
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
      rawBillTextRepository = rawTextRepo,
      embedder = embedder,
      embeddingConfig = embeddingConfig,
      eventPublisher = eventPublisher,
      xa = xa,
      logger = testLogger,
      extractText = (bytes, format) =>
        repcheck.ingestion.bills.text.extraction.BillTextExtractor.extractStream[IO](bytes, format),
    )
  }

  // --- WireMock stubs ---

  /** Stubs the Congress.gov bill list endpoint to return a single bill. */
  private def stubBillListApi(
    congress: Int,
    billType: String,
    number: String,
    title: String,
  ): Unit = {
    val detailUrl = s"$wmBaseUrl/v3/bill/$congress/$billType/$number"
    val _ = wireMock.stubFor(
      get(urlPathEqualTo("/v3/bill"))
        .willReturn(
          aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/json")
            .withBody(s"""{
               |  "items": [
               |    {
               |      "congress": $congress,
               |      "number": "$number",
               |      "billType": "$billType",
               |      "originChamber": "House",
               |      "originChamberCode": "H",
               |      "title": "$title",
               |      "url": "$detailUrl",
               |      "updateDate": "2024-06-01T00:00:00Z"
               |    }
               |  ],
               |  "pagination": {"count": 1}
               |}""".stripMargin)
        )
    )
  }

  /** Stubs the Congress.gov bill detail endpoint. */
  private def stubBillDetailApi(
    congress: Int,
    billType: String,
    number: String,
    title: String,
    sponsorBioguideId: String,
  ): Unit = {
    val _ = wireMock.stubFor(
      get(urlPathEqualTo(s"/v3/bill/$congress/$billType/$number"))
        .willReturn(
          aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/json")
            .withBody(s"""{
               |  "bill": {
               |    "congress": $congress,
               |    "number": "$number",
               |    "billType": "$billType",
               |    "originChamber": "House",
               |    "originChamberCode": "H",
               |    "title": "$title",
               |    "url": "$wmBaseUrl/v3/bill/$congress/$billType/$number",
               |    "updateDate": "2024-06-01T00:00:00Z",
               |    "introducedDate": "2024-01-10",
               |    "sponsors": [
               |      {
               |        "bioguideId": "$sponsorBioguideId",
               |        "fullName": "Rep. Test Sponsor"
               |      }
               |    ]
               |  }
               |}""".stripMargin)
        )
    )
  }

  /** Stubs the Congress.gov text versions endpoint. */
  private def stubTextVersionsApi(
    congress: Int,
    billType: String,
    number: String,
    textUrl: String,
  ): Unit = {
    val _ = wireMock.stubFor(
      get(urlPathEqualTo(s"/v3/bill/$congress/$billType/$number/text"))
        .willReturn(
          aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/json")
            .withBody(s"""{
               |  "textVersions": [
               |    {
               |      "date": "2024-01-15T00:00:00Z",
               |      "type": "IH",
               |      "formats": [
               |        {"type": "Formatted Text", "url": "$textUrl"}
               |      ]
               |    }
               |  ]
               |}""".stripMargin)
        )
    )
  }

  /** Stubs a text download endpoint. */
  private def stubTextDownload(path: String, content: String): Unit = {
    val _ = wireMock.stubFor(
      get(urlPathEqualTo(path))
        .willReturn(
          aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "text/html")
            .withBody(content)
        )
    )
  }

  /** Stubs the Ollama embedding API to return a known embedding. */
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

  private val billTextHtml: String =
    """<html><body><pre>
      |SECTION 1. SHORT TITLE.
      |This Act may be cited as the "Lifecycle Test Act".
      |
      |SECTION 2. FINDINGS.
      |Congress finds the following:
      |(1) End-to-end testing validates the entire ingestion pipeline.
      |</pre></body></html>""".stripMargin

  // --- Tests ---

  "Full lifecycle" should "ingest metadata, check text, download, and store" taggedAs DockerRequired in {
    // Step 0: Stub all WireMock endpoints
    stubBillListApi(118, "hr", "900", "Lifecycle Test Act")
    stubBillDetailApi(118, "hr", "900", "Lifecycle Test Act", "TEST-M001")
    val textPath = "/text/118/hr/900/ih"
    val textUrl  = s"$wmBaseUrl$textPath"
    stubTextVersionsApi(118, "hr", "900", textUrl)
    stubTextDownload(textPath, billTextHtml)

    // Step 1: Metadata processor fetches from Congress.gov (WireMock) and stores in DB
    val metadataProcessor = buildMetadataProcessor()
    val metadataResults   = metadataProcessor.streamAll(0L).compile.toList.unsafeRunSync()
    val _                 = metadataResults.size shouldBe 1
    val _                 = metadataResults.headOption.exists(_.isSucceeded) shouldBe true

    // Step 2: Verify bill is now in the DB
    val storedBill = billRepo.findByBillId("118-HR-900").transact(xa).unsafeRunSync()
    val _          = storedBill.isDefined shouldBe true
    val bill       = storedBill.getOrElse(fail("Bill not found in DB"))
    val _          = bill.title shouldBe "Lifecycle Test Act"
    val dbBillId   = bill.billId

    // Step 3: Text checker finds the bill needs text, publishes event
    val checker        = buildChecker()
    val checkerResults = checker.checkAll(0L).compile.toList.unsafeRunSync()
    val _              = checkerResults.size shouldBe 1
    val _              = checkerResults.headOption.exists(_.isSucceeded) shouldBe true

    // Step 4: Pull event from Pub/Sub emulator
    val messages  = pullMessages()
    val _         = messages.size shouldBe 1
    val eventData = messages.headOption.map(_.getData.toStringUtf8).getOrElse("")
    val eventJson = parse(eventData).getOrElse(fail("Failed to parse event"))
    val payload   = eventJson.hcursor.downField("payload")
    val event = BillTextAvailableEvent(
      naturalKey = payload.downField("naturalKey").as[String].getOrElse(""),
      congress = payload.downField("congress").as[Int].getOrElse(0),
      textUrl = payload.downField("textUrl").as[String].getOrElse(""),
      textFormat = payload.downField("textFormat").as[String].getOrElse(""),
      versionCode = payload.downField("versionCode").as[String].getOrElse(""),
      previousVersionCode = payload.downField("previousVersionCode").as[String].toOption,
    )
    val _ = event.naturalKey shouldBe "118-HR-900"

    // Step 5: Text processor downloads and stores text (no embedding)
    val processor = buildProcessor(withEmbedding = false)
    val result    = processor.processEvent(event, UUID.randomUUID()).unsafeRunSync()
    val _         = result.isSucceeded shouldBe true

    // Step 6: Verify text version stored in DB. Content lives on raw_bill_text chunks post P6.H4c.
    val versions = textVersionRepo.findByBillId(dbBillId).transact(xa).unsafeRunSync()
    val _        = versions.size shouldBe 1
    val stored   = versions.headOption.getOrElse(fail("No text version stored"))
    val _        = stored.versionCode shouldBe "IH"
    val chunks   = rawTextRepo.findByVersionId(stored.id).transact(xa).unsafeRunSync()
    chunks.map(_.content).mkString should include("Lifecycle Test Act")
  }

  it should "support full chain with embedding and vector search" taggedAs DockerRequired in {
    val knownEmbedding = Array.fill(1024)(0.0f).updated(0, 1.0f)

    // Stub all endpoints including Ollama embedding
    stubBillListApi(118, "hr", "901", "Vector Search Lifecycle Bill")
    stubBillDetailApi(118, "hr", "901", "Vector Search Lifecycle Bill", "TEST-M001")
    val textPath = "/text/118/hr/901/ih"
    val textUrl  = s"$wmBaseUrl$textPath"
    stubTextVersionsApi(118, "hr", "901", textUrl)
    stubTextDownload(textPath, billTextHtml)
    stubOllamaEmbedding(knownEmbedding)

    // Step 1: Metadata ingestion
    val metadataProcessor = buildMetadataProcessor()
    val metadataResults   = metadataProcessor.streamAll(0L).compile.toList.unsafeRunSync()
    val _                 = metadataResults.headOption.exists(_.isSucceeded) shouldBe true

    // Step 2: Get bill ID from DB
    val dbBillId = billRepo
      .findByBillId("118-HR-901")
      .transact(xa)
      .unsafeRunSync()
      .map(_.billId)
      .getOrElse(fail("Bill not stored"))

    // Step 3: Checker → Pub/Sub → Processor (with embedding)
    val checker   = buildChecker()
    val _         = checker.checkAll(0L).compile.toList.unsafeRunSync()
    val messages  = pullMessages()
    val eventData = messages.headOption.map(_.getData.toStringUtf8).getOrElse("")
    val eventJson = parse(eventData).getOrElse(fail("Failed to parse event"))
    val p         = eventJson.hcursor.downField("payload")
    val event = BillTextAvailableEvent(
      naturalKey = p.downField("naturalKey").as[String].getOrElse(""),
      congress = p.downField("congress").as[Int].getOrElse(0),
      textUrl = p.downField("textUrl").as[String].getOrElse(""),
      textFormat = p.downField("textFormat").as[String].getOrElse(""),
      versionCode = p.downField("versionCode").as[String].getOrElse(""),
      previousVersionCode = p.downField("previousVersionCode").as[String].toOption,
    )

    val processor = buildProcessor(withEmbedding = true)
    val _         = processor.processEvent(event, UUID.randomUUID()).unsafeRunSync()

    // Step 4: Verify embedding stored on at least one chunk row
    val versions   = textVersionRepo.findByBillId(dbBillId).transact(xa).unsafeRunSync()
    val storedV    = versions.headOption.getOrElse(fail("No text version stored"))
    val storedRows = rawTextRepo.findByVersionId(storedV.id).transact(xa).unsafeRunSync()
    val _          = storedRows.exists(_.embedding.isDefined) shouldBe true

    // Step 5: pgvector cosine similarity search against raw_bill_text
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

  it should "find the right bill via cross-bill vector search" taggedAs DockerRequired in {
    // Two bills with different embeddings
    val embedding1 = Array.fill(1024)(0.0f).updated(0, 1.0f)
    val embedding2 = Array.fill(1024)(0.0f).updated(1, 1.0f)

    // --- Bill 902 ---
    stubBillListApi(118, "hr", "902", "First Vector Bill")
    stubBillDetailApi(118, "hr", "902", "First Vector Bill", "TEST-M001")
    stubTextVersionsApi(118, "hr", "902", s"$wmBaseUrl/text/118/hr/902/ih")
    stubTextDownload("/text/118/hr/902/ih", billTextHtml)
    stubOllamaEmbedding(embedding1)

    val mp1 = buildMetadataProcessor()
    val _   = mp1.streamAll(0L).compile.toList.unsafeRunSync()

    val billId1 = billRepo
      .findByBillId("118-HR-902")
      .transact(xa)
      .unsafeRunSync()
      .map(_.billId)
      .getOrElse(fail("Bill 902 not stored"))

    val checker1 = buildChecker()
    val _        = checker1.checkAll(0L).compile.toList.unsafeRunSync()
    val msgs1    = pullMessages()
    val ed1      = msgs1.headOption.map(_.getData.toStringUtf8).getOrElse("")
    val ej1      = parse(ed1).getOrElse(fail("Parse failed"))
    val p1       = ej1.hcursor.downField("payload")
    val evt1 = BillTextAvailableEvent(
      naturalKey = p1.downField("naturalKey").as[String].getOrElse(""),
      congress = p1.downField("congress").as[Int].getOrElse(0),
      textUrl = p1.downField("textUrl").as[String].getOrElse(""),
      textFormat = p1.downField("textFormat").as[String].getOrElse(""),
      versionCode = p1.downField("versionCode").as[String].getOrElse(""),
      previousVersionCode = p1.downField("previousVersionCode").as[String].toOption,
    )
    val proc1 = buildProcessor(withEmbedding = true)
    val _     = proc1.processEvent(evt1, UUID.randomUUID()).unsafeRunSync()

    // --- Bill 903 (needs fresh stubs) ---
    wireMock.resetMappings()
    stubBillListApi(118, "hr", "903", "Second Vector Bill")
    stubBillDetailApi(118, "hr", "903", "Second Vector Bill", "TEST-M002")
    stubTextVersionsApi(118, "hr", "903", s"$wmBaseUrl/text/118/hr/903/ih")
    stubTextDownload("/text/118/hr/903/ih", billTextHtml)
    stubOllamaEmbedding(embedding2)

    val mp2 = buildMetadataProcessor()
    val _   = mp2.streamAll(0L).compile.toList.unsafeRunSync()

    val checker2 = buildChecker()
    val _        = checker2.checkAll(0L).compile.toList.unsafeRunSync()

    drainMessages()
    // The checker may have found bill 902 again (already has text now) and bill 903.
    // We need to process only the event for bill 903.
    // Re-run checker with fresh stubs that only return bill 903
    wireMock.resetMappings()
    stubBillListApi(118, "hr", "903", "Second Vector Bill")
    stubBillDetailApi(118, "hr", "903", "Second Vector Bill", "TEST-M002")
    stubTextVersionsApi(118, "hr", "903", s"$wmBaseUrl/text/118/hr/903/ih")
    stubTextDownload("/text/118/hr/903/ih", billTextHtml)
    stubOllamaEmbedding(embedding2)

    val checker3 = buildChecker()
    val _        = checker3.checkAll(0L).compile.toList.unsafeRunSync()
    val msgs2    = pullMessages()

    // Find the event for bill 903
    val bill903Msg = msgs2
      .find { m =>
        val data = m.getData.toStringUtf8
        parse(data).toOption
          .flatMap(_.hcursor.downField("payload").downField("naturalKey").as[String].toOption)
          .contains("118-HR-903")
      }
      .getOrElse(fail("No event for bill 903"))

    val ed2 = bill903Msg.getData.toStringUtf8
    val ej2 = parse(ed2).getOrElse(fail("Parse failed"))
    val p2  = ej2.hcursor.downField("payload")
    val evt2 = BillTextAvailableEvent(
      naturalKey = p2.downField("naturalKey").as[String].getOrElse(""),
      congress = p2.downField("congress").as[Int].getOrElse(0),
      textUrl = p2.downField("textUrl").as[String].getOrElse(""),
      textFormat = p2.downField("textFormat").as[String].getOrElse(""),
      versionCode = p2.downField("versionCode").as[String].getOrElse(""),
      previousVersionCode = p2.downField("previousVersionCode").as[String].toOption,
    )
    val proc2 = buildProcessor(withEmbedding = true)
    val _     = proc2.processEvent(evt2, UUID.randomUUID()).unsafeRunSync()

    // --- Vector search: query with embedding1 should rank bill 902 first.
    // Embeddings now live on raw_bill_text rows (P6.H4c).
    val queryVector = embedding1.mkString("[", ",", "]")
    val results = sql"""
      SELECT rbt.bill_id, 1 - (rbt.embedding <=> $queryVector::vector) as similarity
      FROM raw_bill_text rbt
      WHERE rbt.embedding IS NOT NULL
      ORDER BY rbt.embedding <=> $queryVector::vector
      LIMIT 2
    """.query[(Long, Double)].to[List].transact(xa).unsafeRunSync()

    val _ = results.size shouldBe 2
    // First: bill 902 (identical vector, similarity ~1.0)
    val _ = results.headOption.map(_._1).getOrElse(0L) shouldBe billId1
    val _ = results.headOption.map(_._2).getOrElse(0.0) should be > 0.99
    // Second: bill 903 (orthogonal, similarity ~0.0)
    results.lastOption.map(_._2).getOrElse(1.0) should be < 0.01
  }

}
