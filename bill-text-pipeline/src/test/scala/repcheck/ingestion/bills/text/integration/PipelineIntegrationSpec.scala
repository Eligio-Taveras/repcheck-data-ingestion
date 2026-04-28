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
import repcheck.ingestion.bills.text.embedding.{
  CrossBillEmbedder,
  EmbeddingConfig,
  EmbeddingService,
  NoOpEmbeddingService,
  OllamaEmbeddingService,
}
import repcheck.ingestion.bills.text.persistence.DoobieRawBillTextRepository
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
  private val rawTextRepo     = new DoobieRawBillTextRepository()

  private val defaultEmbeddingConfig: EmbeddingConfig = EmbeddingConfig(
    baseUrl = "http://127.0.0.1:0",
    modelName = "bill-text-embedding",
    dimensions = 1024,
    timeoutSeconds = 10,
    maxChunkChars = 30000,
    embedBatchSize = 10,
    embedBatchTimeout = scala.concurrent.duration.DurationInt(1).second,
    embedQueueCapacityMultiplier = 10,
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

  private def buildProcessor(
    embeddingService: EmbeddingService[IO],
    embeddingConfig: EmbeddingConfig = defaultEmbeddingConfig,
  ): BillTextProcessor[IO] = {
    val downloader = new BillTextDownloader[IO](
      client = httpClient,
      govInfoApiKey = "integration-test-key",
      govInfoBaseUrl = "https://api.govinfo.gov",
      logger = testLogger,
    )
    val pubsubPublisher = new GooglePubSubEventPublisher[IO](publisher)
    val eventPublisher =
      new DefaultIngestionEventPublisher[IO](
        pubsubPublisher,
        topicName.toString,
        "integration-test",
        new RetryWrapper[IO]((_, _, _, _, _, _) => IO.unit),
        RetryConfig(),
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

  private def buildProcessorNoEmbed(): BillTextProcessor[IO] =
    buildProcessor(new NoOpEmbeddingService[IO])

  private def buildProcessorWithOllama(): BillTextProcessor[IO] = {
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
    val ollamaService = new OllamaEmbeddingService[IO](httpClient, embeddingConfig, testLogger)
    buildProcessor(ollamaService, embeddingConfig)
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
    // Content now lives in raw_bill_text chunk rows.
    val chunks = rawTextRepo.findByVersionId(stored.id).transact(xa).unsafeRunSync()
    chunks.map(_.content).mkString should include("Test Act")
  }

  it should "store embedding and allow pgvector similarity search" taggedAs DockerRequired in {
    val dbBillId = seedBill("118-HR-102", number = "102")
    val textPath = "/text/118/hr/102/ih"
    stubTextDownload(textPath, sampleHtml)

    // Create a known 1024-dim embedding (unit vector in first dimension)
    val knownEmbedding = Array.fill(1024)(0.0f).updated(0, 1.0f)
    stubOllamaEmbedding(knownEmbedding)

    val event = makeEvent(
      naturalKey = "118-HR-102",
      textUrl = s"http://127.0.0.1:${wireMock.port().toString}$textPath",
    )
    val processor = buildProcessorWithOllama()
    val _         = processor.processEvent(event, UUID.randomUUID()).unsafeRunSync()

    // Verify embedding stored on at least one raw_bill_text chunk row
    val versions   = textVersionRepo.findByBillId(dbBillId).transact(xa).unsafeRunSync()
    val storedVer  = versions.headOption.getOrElse(fail("No version stored"))
    val storedRows = rawTextRepo.findByVersionId(storedVer.id).transact(xa).unsafeRunSync()
    val _          = storedRows.exists(_.embedding.isDefined) shouldBe true

    // Verify similarity search works via pgvector against raw_bill_text
    val queryVector = knownEmbedding.mkString("[", ",", "]")
    val similarity = sql"""
      SELECT 1 - (embedding <=> $queryVector::vector) as similarity
      FROM raw_bill_text
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

    // Verify text stored on chunk rows, embedding is None on those chunks
    val versions = textVersionRepo.findByBillId(dbBillId).transact(xa).unsafeRunSync()
    val _        = versions.size shouldBe 1
    val stored   = versions.headOption.getOrElse(fail("No version stored"))
    val chunks   = rawTextRepo.findByVersionId(stored.id).transact(xa).unsafeRunSync()
    val _        = chunks.map(_.content).mkString should include("Test Act")
    chunks.forall(_.embedding.isEmpty) shouldBe true
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
    val storedV  = versions.headOption.getOrElse(fail("No version stored"))
    val chunks   = rawTextRepo.findByVersionId(storedV.id).transact(xa).unsafeRunSync()
    val content  = chunks.map(_.content).mkString
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
