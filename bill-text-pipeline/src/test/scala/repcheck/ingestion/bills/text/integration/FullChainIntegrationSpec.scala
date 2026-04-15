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
import repcheck.ingestion.bills.text.config.BillTextPipelineConfig
import repcheck.ingestion.bills.text.download.BillTextDownloader
import repcheck.ingestion.bills.text.embedding.NoOpEmbeddingService
import repcheck.ingestion.bills.text.pipeline.BillTextProcessor
import repcheck.ingestion.bills.textcheck.api.BillTextApiClient
import repcheck.ingestion.bills.textcheck.config.BillTextCheckerConfig
import repcheck.ingestion.bills.textcheck.pipeline.BillTextAvailabilityChecker
import repcheck.ingestion.common.api.CongressGovClientConfig
import repcheck.ingestion.common.events.{DefaultIngestionEventPublisher, GooglePubSubEventPublisher}
import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}
import repcheck.pipeline.models.errors.{RetryConfig, RetryWrapper}
import repcheck.pipeline.models.events.BillTextAvailableEvent
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

  override def afterAll(): Unit = {
    wireMock.stop()
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
    val downloader =
      new BillTextDownloader[IO](httpClient, BillTextPipelineConfig(1, 10, 10485760L, 100.millis), testLogger)
    val pubsubPublisher = new GooglePubSubEventPublisher[IO](publisher)
    val pipelineEventPublisher =
      new DefaultIngestionEventPublisher[IO](
        pubsubPublisher,
        topicName.toString,
        "pipeline-integration",
        new RetryWrapper[IO]((_, _, _, _, _, _) => IO.unit),
        testRetryConfig,
      )

    new BillTextProcessor[IO](
      downloader = downloader,
      billRepository = billRepo,
      textVersionRepository = textVersionRepo,
      embeddingService = new NoOpEmbeddingService[IO],
      eventPublisher = pipelineEventPublisher,
      xa = xa,
      logger = testLogger,
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
    billRepo.upsert(bill).transact(xa).unsafeRunSync()
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
    val checkerResults = checker.checkAll(UUID.randomUUID()).compile.toList.unsafeRunSync()
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

    // Step 4: Verify text stored in DB
    val versions = textVersionRepo.findByBillId(dbBillId).transact(xa).unsafeRunSync()
    val _        = versions.size shouldBe 1
    versions.headOption.flatMap(_.content).getOrElse("") should include("Full Chain Test Act")
  }

  it should "propagate previousVersionCode through the full chain" taggedAs DockerRequired in {
    val dbBillId = seedBill("118-HR-51", number = "51")

    // First, insert a text version so the bill has existing text
    import repcheck.shared.models.congress.bill.TextVersionCode
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
      textUrl = Some("https://old.url"),
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
    val checkerResults = checker.checkAll(UUID.randomUUID()).compile.toList.unsafeRunSync()
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
    val checkerResults = checker.checkAll(UUID.randomUUID()).compile.toList.unsafeRunSync()
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
