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
import repcheck.ingestion.bills.text.embedding.{
  EmbeddingConfig,
  EmbeddingService,
  NoOpEmbeddingService,
  OllamaEmbeddingService,
}
import repcheck.ingestion.bills.text.pipeline.BillTextProcessor
import repcheck.ingestion.common.events.{DefaultIngestionEventPublisher, GooglePubSubEventPublisher}
import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}
import repcheck.pipeline.models.errors.{RetryConfig, RetryWrapper}
import repcheck.pipeline.models.events.BillTextAvailableEvent
import repcheck.shared.models.congress.common.{BillType, Chamber}
import repcheck.shared.models.congress.dos.bill.BillDO

/**
 * Integration tests for the Bill Text Pipeline. Uses real AlloyDB Omni (Docker), Pub/Sub emulator, and WireMock for
 * text downloads and Ollama embedding. Validates the flow: BillTextAvailableEvent → download → embed → store → emit
 * BillTextIngestedEvent.
 */
class PipelineIntegrationSpec
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

  private val pipelineConfig = BillTextPipelineConfig(
    parallelism = 1,
    downloadTimeoutSeconds = 10,
    maxContentBytes = 10485760L,
    pageDelay = 100.millis,
  )

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

  private def buildProcessor(embeddingService: EmbeddingService[IO]): BillTextProcessor[IO] = {
    val downloader      = new BillTextDownloader[IO](httpClient, pipelineConfig, testLogger)
    val pubsubPublisher = new GooglePubSubEventPublisher[IO](publisher)
    val eventPublisher =
      new DefaultIngestionEventPublisher[IO](
        pubsubPublisher,
        topicName.toString,
        "integration-test",
        new RetryWrapper[IO]((_, _, _, _, _, _) => IO.unit),
        RetryConfig(),
      )

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

  private def buildProcessorNoEmbed(): BillTextProcessor[IO] =
    buildProcessor(new NoOpEmbeddingService[IO])

  private def buildProcessorWithOllama(): BillTextProcessor[IO] = {
    val embeddingConfig = EmbeddingConfig(
      baseUrl = s"http://127.0.0.1:${wireMock.port().toString}",
      modelName = "qwen3-embedding",
      dimensions = 1536,
      timeoutSeconds = 10,
    )
    val ollamaService = new OllamaEmbeddingService[IO](httpClient, embeddingConfig, testLogger)
    buildProcessor(ollamaService)
  }

  private def seedBill(naturalKey: String, congress: Int = 118, number: String): Long = {
    val bill = BillDO(
      billId = 0L,
      naturalKey = naturalKey,
      congress = congress,
      billType = BillType.HR,
      number = number,
      title = "Test Bill",
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

  private val sampleHtml: String =
    """<html><body><pre>
      |SECTION 1. SHORT TITLE.
      |This Act may be cited as the "Test Act".
      |
      |SECTION 2. FINDINGS.
      |Congress finds the following:
      |(1) Testing is important.
      |</pre></body></html>""".stripMargin

  private def makeEvent(
    naturalKey: String,
    congress: Int = 118,
    textUrl: String,
    textFormat: String = "Formatted Text",
    versionCode: String = "IH",
    previousVersionCode: Option[String] = None,
  ): BillTextAvailableEvent =
    BillTextAvailableEvent(
      naturalKey = naturalKey,
      congress = congress,
      textUrl = textUrl,
      textFormat = textFormat,
      versionCode = versionCode,
      previousVersionCode = previousVersionCode,
    )

  private def extractPayloadField(messageData: String, field: String): Option[String] =
    parse(messageData).toOption
      .flatMap(_.hcursor.downField("payload").downField(field).as[String].toOption)

  // --- Tests ---

  "PipelineIntegration" should "download text, store version, and emit event" taggedAs DockerRequired in {
    val _        = seedBill("118-HR-100", number = "100")
    val textPath = "/text/118/hr/100/ih"
    stubTextDownload(textPath, sampleHtml)

    val event = makeEvent(
      naturalKey = "118-HR-100",
      textUrl = s"http://127.0.0.1:${wireMock.port().toString}$textPath",
    )
    val processor = buildProcessorNoEmbed()
    val result    = processor.processEvent(event, UUID.randomUUID()).unsafeRunSync()

    val _ = result.isSucceeded shouldBe true

    // Verify downstream event
    val messages = pullMessages()
    messages.size shouldBe 1
  }

  it should "store BillTextVersionDO with correct content in database" taggedAs DockerRequired in {
    val dbBillId = seedBill("118-HR-101", number = "101")
    val textPath = "/text/118/hr/101/ih"
    stubTextDownload(textPath, sampleHtml)

    val event = makeEvent(
      naturalKey = "118-HR-101",
      textUrl = s"http://127.0.0.1:${wireMock.port().toString}$textPath",
      versionCode = "IH",
    )
    val processor = buildProcessorNoEmbed()
    val _         = processor.processEvent(event, UUID.randomUUID()).unsafeRunSync()

    // Query stored version from DB
    val versions =
      textVersionRepo.findByBillId(dbBillId).transact(xa).unsafeRunSync()
    val _      = versions.size shouldBe 1
    val stored = versions.headOption.getOrElse(fail("No version stored"))
    val _      = stored.versionCode shouldBe "IH"
    val _      = stored.billId shouldBe dbBillId
    val _      = stored.url shouldBe Some(s"http://127.0.0.1:${wireMock.port().toString}$textPath")
    stored.content.getOrElse("") should include("Test Act")
  }

  it should "store embedding and allow pgvector similarity search" taggedAs DockerRequired in {
    val dbBillId = seedBill("118-HR-102", number = "102")
    val textPath = "/text/118/hr/102/ih"
    stubTextDownload(textPath, sampleHtml)

    // Create a known 1536-dim embedding (unit vector in first dimension)
    val knownEmbedding = Array.fill(1536)(0.0f).updated(0, 1.0f)
    stubOllamaEmbedding(knownEmbedding)

    val event = makeEvent(
      naturalKey = "118-HR-102",
      textUrl = s"http://127.0.0.1:${wireMock.port().toString}$textPath",
    )
    val processor = buildProcessorWithOllama()
    val _         = processor.processEvent(event, UUID.randomUUID()).unsafeRunSync()

    // Verify embedding stored
    val versions = textVersionRepo.findByBillId(dbBillId).transact(xa).unsafeRunSync()
    val _        = versions.headOption.flatMap(_.embedding).isDefined shouldBe true

    // Verify similarity search works via pgvector
    val queryVector = knownEmbedding.mkString("[", ",", "]")
    val similarity = sql"""
      SELECT 1 - (embedding <=> $queryVector::vector) as similarity
      FROM bill_text_versions
      WHERE bill_id = $dbBillId AND embedding IS NOT NULL
      ORDER BY embedding <=> $queryVector::vector
      LIMIT 1
    """.query[Double].option.transact(xa).unsafeRunSync()

    val _ = similarity.isDefined shouldBe true
    // Cosine similarity of identical vectors should be ~1.0
    similarity.getOrElse(0.0) should be > 0.99
  }

  it should "return Failed when text download returns 404" taggedAs DockerRequired in {
    val _        = seedBill("118-HR-103", number = "103")
    val textPath = "/text/118/hr/103/ih"
    val _ = wireMock.stubFor(
      get(urlPathEqualTo(textPath))
        .willReturn(aResponse().withStatus(404).withBody("Not Found"))
    )

    val event = makeEvent(
      naturalKey = "118-HR-103",
      textUrl = s"http://127.0.0.1:${wireMock.port().toString}$textPath",
    )
    val processor = buildProcessorNoEmbed()
    val result    = processor.processEvent(event, UUID.randomUUID()).unsafeRunSync()

    val _ = result.isFailed shouldBe true

    // No events should be published
    pullMessages() shouldBe empty
  }

  it should "return Failed when text download returns 500" taggedAs DockerRequired in {
    val _        = seedBill("118-HR-104", number = "104")
    val textPath = "/text/118/hr/104/ih"
    val _ = wireMock.stubFor(
      get(urlPathEqualTo(textPath))
        .willReturn(aResponse().withStatus(500).withBody("Internal Server Error"))
    )

    val event = makeEvent(
      naturalKey = "118-HR-104",
      textUrl = s"http://127.0.0.1:${wireMock.port().toString}$textPath",
    )
    val processor = buildProcessorNoEmbed()
    val result    = processor.processEvent(event, UUID.randomUUID()).unsafeRunSync()

    val _ = result.isFailed shouldBe true
    pullMessages() shouldBe empty
  }

  it should "store text without embedding when embedding service fails" taggedAs DockerRequired in {
    val dbBillId = seedBill("118-HR-105", number = "105")
    val textPath = "/text/118/hr/105/ih"
    stubTextDownload(textPath, sampleHtml)

    // Stub Ollama to return 500 — OllamaEmbeddingService gracefully returns None
    val _ = wireMock.stubFor(
      post(urlEqualTo("/api/embed"))
        .willReturn(aResponse().withStatus(500).withBody("Internal Server Error"))
    )

    val event = makeEvent(
      naturalKey = "118-HR-105",
      textUrl = s"http://127.0.0.1:${wireMock.port().toString}$textPath",
    )
    val processor = buildProcessorWithOllama()
    val result    = processor.processEvent(event, UUID.randomUUID()).unsafeRunSync()

    val _ = result.isSucceeded shouldBe true

    // Verify text stored, embedding is None
    val versions = textVersionRepo.findByBillId(dbBillId).transact(xa).unsafeRunSync()
    val _        = versions.size shouldBe 1
    val stored   = versions.headOption.getOrElse(fail("No version stored"))
    val _        = stored.content.getOrElse("") should include("Test Act")
    stored.embedding.isDefined shouldBe false
  }

  it should "return Failed when bill is not found in database" taggedAs DockerRequired in {
    // Do NOT seed any bill — processor should fail on lookup
    val event = makeEvent(
      naturalKey = "118-HR-999",
      textUrl = s"http://127.0.0.1:${wireMock.port().toString}/text/does-not-matter",
    )
    val processor = buildProcessorNoEmbed()
    val result    = processor.processEvent(event, UUID.randomUUID()).unsafeRunSync()

    val _ = result.isFailed shouldBe true
    pullMessages() shouldBe empty
  }

  it should "strip HTML tags from stored text content" taggedAs DockerRequired in {
    val dbBillId = seedBill("118-HR-106", number = "106")
    val textPath = "/text/118/hr/106/ih"
    val richHtml = """<html><body><pre><b>SECTION 1.</b> SHORT TITLE.
      |This Act may be cited as the "<i>Clean Air Act</i>".
      |</pre></body></html>""".stripMargin
    stubTextDownload(textPath, richHtml)

    val event = makeEvent(
      naturalKey = "118-HR-106",
      textUrl = s"http://127.0.0.1:${wireMock.port().toString}$textPath",
    )
    val processor = buildProcessorNoEmbed()
    val _         = processor.processEvent(event, UUID.randomUUID()).unsafeRunSync()

    val versions = textVersionRepo.findByBillId(dbBillId).transact(xa).unsafeRunSync()
    val content  = versions.headOption.flatMap(_.content).getOrElse("")
    // Content should not contain HTML tags
    val _ = content should not include "<b>"
    val _ = content should not include "<i>"
    content should include("Clean Air Act")
  }

  it should "emit BillTextIngestedEvent with correct payload fields" taggedAs DockerRequired in {
    val _        = seedBill("118-HR-107", number = "107")
    val textPath = "/text/118/hr/107/rh"
    stubTextDownload(textPath, sampleHtml)

    val correlationId = UUID.randomUUID()
    val event = makeEvent(
      naturalKey = "118-HR-107",
      congress = 118,
      textUrl = s"http://127.0.0.1:${wireMock.port().toString}$textPath",
      versionCode = "RH",
      previousVersionCode = Some("IH"),
    )
    val processor = buildProcessorNoEmbed()
    val _         = processor.processEvent(event, correlationId).unsafeRunSync()

    val messages = pullMessages()
    val _        = messages.size shouldBe 1
    val data     = messages.headOption.map(_.getData.toStringUtf8).getOrElse("")
    val _        = extractPayloadField(data, "naturalKey") shouldBe Some("118-HR-107")
    val _        = extractPayloadField(data, "versionCode") shouldBe Some("RH")
    extractPayloadField(data, "previousVersionCode") shouldBe Some("IH")
  }

}
