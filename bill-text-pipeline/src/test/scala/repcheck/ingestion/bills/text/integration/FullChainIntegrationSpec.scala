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
import repcheck.ingestion.bills.common.persistence.{DoobieBillRepository, DoobieBillTextVersionRepository}
import repcheck.ingestion.bills.common.testing.{DockerRequired, PubSubEmulatorFixture, TransactorFixture}
import repcheck.ingestion.bills.text.download.BillTextDownloader
import repcheck.ingestion.bills.text.embedding.{CrossBillEmbedder, EmbeddingConfig, NoOpEmbeddingService}
import repcheck.ingestion.bills.text.persistence.DoobieRawBillTextRepository
import repcheck.ingestion.bills.text.pipeline.BillTextProcessor
import repcheck.ingestion.bills.textcheck.api.BillTextApiClient
import repcheck.ingestion.bills.textcheck.config.BillTextCheckerConfig
import repcheck.ingestion.bills.textcheck.pipeline.BillTextAvailabilityChecker
import repcheck.ingestion.common.api.CongressGovClientConfig
import repcheck.ingestion.common.events.{DefaultIngestionEventPublisher, GooglePubSubEventPublisher}
import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}
import repcheck.pipeline.models.errors.{RetryConfig, RetryWrapper}
import repcheck.pipeline.models.events.BillTextAvailableEvent
import repcheck.shared.models.congress.bill.TextVersionCode
import repcheck.shared.models.congress.common.{BillType, Chamber}
import repcheck.shared.models.congress.dos.bill.BillDO

/**
 * Full-chain integration tests: checker finds bills needing text → emits event → pipeline processes event → stores text
 * in DB → emits downstream event. Uses real AlloyDB Omni (Docker), Pub/Sub emulator, and WireMock for Congress.gov API
 * and text download endpoints.
 */
class FullChainIntegrationSpec
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

  private val billRepo        = new DoobieBillRepository()
  private val textVersionRepo = new DoobieBillTextVersionRepository()
  private val rawTextRepo     = new DoobieRawBillTextRepository()

  private val embeddingConfigStub: EmbeddingConfig = EmbeddingConfig(
    baseUrl = "http://127.0.0.1:0",
    modelName = "bill-text-embedding",
    dimensions = 1024,
    timeoutSeconds = 10,
    maxChunkChars = 30000,
    embedBatchSize = 10,
    embedBatchTimeout = scala.concurrent.duration.DurationInt(1).second,
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

  private def buildChecker(): BillTextAvailabilityChecker[IO] = {
    val retryWrapper = new RetryWrapper[IO]((_, _, _, _, _, _) => IO.unit)
    val congressConfig = CongressGovClientConfig(
      apiKey = "test-api-key",
      baseUrl = s"http://127.0.0.1:${wireMock.port().toString}/v3",
      pageSize = 250,
      pageDelay = Duration.Zero,
      retry = testRetryConfig,
    )
    val textApiClient   = new BillTextApiClient[IO](congressConfig, httpClient, retryWrapper)
    val pubsubPublisher = new GooglePubSubEventPublisher[IO](publisher)
    val checkerEventPublisher =
      new DefaultIngestionEventPublisher[IO](
        pubsubPublisher,
        topicName.toString,
        "checker-integration",
        new RetryWrapper[IO]((_, _, _, _, _, _) => IO.unit),
        testRetryConfig,
      )

    new BillTextAvailabilityChecker[IO](
      textApiClient = textApiClient,
      billRepo = billRepo,
      eventPublisher = checkerEventPublisher,
      retryWrapper = retryWrapper,
      xa = xa,
      config = BillTextCheckerConfig(parallelism = 1, eventPublishRetry = testRetryConfig),
      logger = testLogger,
    )
  }

  private def buildProcessor(): BillTextProcessor[IO] = {
    val downloader      = new BillTextDownloader[IO](httpClient, testLogger)
    val pubsubPublisher = new GooglePubSubEventPublisher[IO](publisher)
    val pipelineEventPublisher =
      new DefaultIngestionEventPublisher[IO](
        pubsubPublisher,
        topicName.toString,
        "pipeline-integration",
        new RetryWrapper[IO]((_, _, _, _, _, _) => IO.unit),
        testRetryConfig,
      )

    val (embedder, fin) = CrossBillEmbedder
      .resource[IO](
        embeddingService = new NoOpEmbeddingService[IO],
        rawBillTextRepository = rawTextRepo,
        xa = xa,
        logger = testLogger,
        embedBatchSize = embeddingConfigStub.embedBatchSize,
        embedBatchTimeout = embeddingConfigStub.embedBatchTimeout,
        queueCapacity = embeddingConfigStub.embedBatchSize * 10,
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
      embeddingConfig = embeddingConfigStub,
      eventPublisher = pipelineEventPublisher,
      xa = xa,
      logger = testLogger,
      extractText = (bytes, format) =>
        repcheck.ingestion.bills.text.extraction.BillTextExtractor.extractStream[IO](bytes, format),
    )
  }

  private def seedBill(naturalKey: String, congress: Int = 118, number: String): Long = {
    val bill = BillDO(
      billId = 0L,
      naturalKey = naturalKey,
      congress = congress,
      billType = BillType.HR,
      number = number,
      title = "Full Chain Test Bill",
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
    val billId = billRepo.upsert(bill).transact(xa).unsafeRunSync()
    // PR #77 introduced a stage-aware sweep filter:
    //   WHERE expected_text_version_code IS NOT NULL
    //     AND text_version_type IS DISTINCT FROM expected_text_version_code
    // For these tests to exercise the checker → pipeline chain we need an expected stage set; bills
    // that get to this fixture are introduced (latestActionDate = None) so IH is the right floor.
    val _ = billRepo.updateExpectedVersion(naturalKey, TextVersionCode.IH).transact(xa).unsafeRunSync()
    billId
  }

  private val billTextHtml: String =
    """<html><body><pre>
      |SECTION 1. SHORT TITLE.
      |This Act may be cited as the "Full Chain Test Act".
      |
      |SECTION 2. PURPOSE.
      |To validate the complete bill text ingestion pipeline.
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

  private def extractPayloadField(messageData: String, field: String): Option[String] =
    parse(messageData).toOption
      .flatMap(_.hcursor.downField("payload").downField(field).as[String].toOption)

  // --- Tests ---

  "FullChain integration" should "run checker then pipeline end-to-end" taggedAs DockerRequired in {
    val dbBillId = seedBill("118-HR-50", number = "50")
    val textUrl  = s"http://127.0.0.1:${wireMock.port().toString}/text/118/hr/50/ih"

    // Stub Congress.gov text versions API
    val _ = wireMock.stubFor(
      get(urlPathEqualTo("/v3/bill/118/hr/50/text"))
        .willReturn(
          aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/json")
            .withBody(textVersionsJson(textUrl))
        )
    )
    // Stub bill text download
    val _ = wireMock.stubFor(
      get(urlPathEqualTo("/text/118/hr/50/ih"))
        .willReturn(
          aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "text/html")
            .withBody(billTextHtml)
        )
    )

    // Step 1: Run checker — finds bill needing text, emits BillTextAvailableEvent
    val checker        = buildChecker()
    val checkerResults = checker.checkAll(0L).compile.toList.unsafeRunSync()
    val _              = checkerResults.size shouldBe 1
    val _              = checkerResults.headOption.exists(_.isSucceeded) shouldBe true

    // Step 2: Pull event from Pub/Sub and parse it
    val checkerMessages = pullMessages()
    val _               = checkerMessages.size shouldBe 1
    val eventData       = checkerMessages.headOption.map(_.getData.toStringUtf8).getOrElse("")
    val eventJson       = parse(eventData).getOrElse(fail("Failed to parse event JSON"))
    val payload         = eventJson.hcursor.downField("payload")
    val event = BillTextAvailableEvent(
      naturalKey = payload.downField("naturalKey").as[String].getOrElse(""),
      congress = payload.downField("congress").as[Int].getOrElse(0),
      textUrl = payload.downField("textUrl").as[String].getOrElse(""),
      textFormat = payload.downField("textFormat").as[String].getOrElse(""),
      versionCode = payload.downField("versionCode").as[String].getOrElse(""),
      previousVersionCode = payload.downField("previousVersionCode").as[String].toOption,
    )

    // Step 3: Run pipeline processor with the event
    val processor      = buildProcessor()
    val pipelineResult = processor.processEvent(event, UUID.randomUUID()).unsafeRunSync()
    val _              = pipelineResult.isSucceeded shouldBe true

    // Step 4: Verify text stored in DB. Content lives on raw_bill_text chunks post P6.H4c.
    val versions = textVersionRepo.findByBillId(dbBillId).transact(xa).unsafeRunSync()
    val _        = versions.size shouldBe 1
    val storedV  = versions.headOption.getOrElse(fail("No version stored"))
    val chunks   = rawTextRepo.findByVersionId(storedV.id).transact(xa).unsafeRunSync()
    chunks.map(_.content).mkString should include("Full Chain Test Act")
  }

  it should "propagate previousVersionCode through the full chain" taggedAs DockerRequired in {
    val dbBillId = seedBill("118-HR-51", number = "51")

    // First, insert a text version so the bill has existing text.
    //
    // Post-#77 the bill-text-availability-checker filter is the stage-aware
    //   WHERE expected_text_version_code IS NOT NULL
    //     AND text_version_type IS DISTINCT FROM expected_text_version_code
    // We need:
    //   * text_version_type = IH (the bill is currently stored at the IH stage)
    //   * expected_text_version_code = RH (CRS / bill-summary-pipeline has advanced the expected
    //     stage to RH because that's what the API now reports as available)
    // After this PR's seedBill change, `dbBillId` was upserted with expected = IH; we override to
    // RH below so the DISTINCT-FROM check fires and the checker picks the bill up. The simulated
    // state here is the steady state right after bill-summary advances expected from IH → RH but
    // before bill-text-pipeline downloads the RH formatted text and updates text_version_type.
    val billWithText = BillDO(
      billId = dbBillId,
      naturalKey = "118-HR-51",
      congress = 118,
      billType = BillType.HR,
      number = "51",
      title = "Full Chain Test Bill",
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
      textVersionType = Some(TextVersionCode.IH),
      textDate = None,
      textContent = None,
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
    val _ = billRepo.upsert(billWithText).transact(xa).unsafeRunSync()
    // Override the expected_text_version_code that seedBill defaulted to IH so the sweep filter
    // fires (text_version_type = IH != expected = RH).
    val _ = billRepo.updateExpectedVersion("118-HR-51", TextVersionCode.RH).transact(xa).unsafeRunSync()

    val textUrl = s"http://127.0.0.1:${wireMock.port().toString}/text/118/hr/51/rh"
    // API returns newer version (RH instead of IH)
    val _ = wireMock.stubFor(
      get(urlPathEqualTo("/v3/bill/118/hr/51/text"))
        .willReturn(
          aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/json")
            .withBody(s"""{
               |  "textVersions": [
               |    {
               |      "date": "2024-02-01T00:00:00Z",
               |      "type": "RH",
               |      "formats": [{"type": "Formatted Text", "url": "$textUrl"}]
               |    }
               |  ]
               |}""".stripMargin)
        )
    )
    val _ = wireMock.stubFor(
      get(urlPathEqualTo("/text/118/hr/51/rh"))
        .willReturn(
          aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "text/html")
            .withBody(billTextHtml)
        )
    )

    // Run checker
    val checker        = buildChecker()
    val checkerResults = checker.checkAll(0L).compile.toList.unsafeRunSync()
    val _              = checkerResults.size shouldBe 1
    val _              = checkerResults.headOption.exists(_.isSucceeded) shouldBe true

    // Pull and parse checker event
    val messages  = pullMessages()
    val _         = messages.size shouldBe 1
    val eventData = messages.headOption.map(_.getData.toStringUtf8).getOrElse("")
    val _         = extractPayloadField(eventData, "previousVersionCode") shouldBe Some("IH")
    extractPayloadField(eventData, "versionCode") shouldBe Some("RH")
  }

  it should "handle multiple bills through the full chain" taggedAs DockerRequired in {
    val billId1 = seedBill("118-HR-60", number = "60")
    val billId2 = seedBill("118-HR-61", number = "61")

    val textUrl1 = s"http://127.0.0.1:${wireMock.port().toString}/text/118/hr/60/ih"
    val textUrl2 = s"http://127.0.0.1:${wireMock.port().toString}/text/118/hr/61/ih"

    // Stub API for both bills
    val _ = wireMock.stubFor(
      get(urlPathEqualTo("/v3/bill/118/hr/60/text"))
        .willReturn(
          aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/json")
            .withBody(textVersionsJson(textUrl1))
        )
    )
    val _ = wireMock.stubFor(
      get(urlPathEqualTo("/v3/bill/118/hr/61/text"))
        .willReturn(
          aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/json")
            .withBody(textVersionsJson(textUrl2))
        )
    )
    // Stub text downloads
    val _ = wireMock.stubFor(
      get(urlPathEqualTo("/text/118/hr/60/ih"))
        .willReturn(aResponse().withStatus(200).withBody(billTextHtml))
    )
    val _ = wireMock.stubFor(
      get(urlPathEqualTo("/text/118/hr/61/ih"))
        .willReturn(aResponse().withStatus(200).withBody(billTextHtml))
    )

    // Run checker
    val checker        = buildChecker()
    val checkerResults = checker.checkAll(0L).compile.toList.unsafeRunSync()
    val _              = checkerResults.size shouldBe 2
    val _              = checkerResults.count(_.isSucceeded) shouldBe 2

    // Pull checker events and process each through pipeline
    val events    = pullMessages()
    val _         = events.size shouldBe 2
    val processor = buildProcessor()

    events.foreach { msg =>
      val data = msg.getData.toStringUtf8
      val json = parse(data).getOrElse(fail("Failed to parse"))
      val p    = json.hcursor.downField("payload")
      val event = BillTextAvailableEvent(
        naturalKey = p.downField("naturalKey").as[String].getOrElse(""),
        congress = p.downField("congress").as[Int].getOrElse(0),
        textUrl = p.downField("textUrl").as[String].getOrElse(""),
        textFormat = p.downField("textFormat").as[String].getOrElse(""),
        versionCode = p.downField("versionCode").as[String].getOrElse(""),
        previousVersionCode = p.downField("previousVersionCode").as[String].toOption,
      )
      val result = processor.processEvent(event, UUID.randomUUID()).unsafeRunSync()
      val _      = result.isSucceeded shouldBe true
    }

    // Verify both bills have text versions stored
    val versions1 = textVersionRepo.findByBillId(billId1).transact(xa).unsafeRunSync()
    val versions2 = textVersionRepo.findByBillId(billId2).transact(xa).unsafeRunSync()
    val _         = versions1.size shouldBe 1
    versions2.size shouldBe 1
  }

}
