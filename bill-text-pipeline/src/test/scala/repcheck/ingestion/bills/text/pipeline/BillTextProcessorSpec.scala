package repcheck.ingestion.bills.text.pipeline

import java.util.UUID

import cats.effect.IO
import cats.effect.unsafe.implicits.global

import doobie._

import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.{any, anyString}
import org.mockito.Mockito.{never, times, verify, when}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar
import repcheck.ingestion.bills.common.persistence.{BillRepository, BillTextVersionRepository}
import repcheck.ingestion.bills.text.download.BillTextDownloader
import repcheck.ingestion.bills.text.embedding.{EmbeddingConfig, EmbeddingGenerationFailed, EmbeddingService}
import repcheck.ingestion.bills.text.errors.{BillTextProcessingFailed, TextDownloadFailed}
import repcheck.ingestion.bills.text.persistence.RawBillTextRepository
import repcheck.ingestion.common.events.IngestionEventPublisher
import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}
import repcheck.pipeline.models.events.{BillTextAvailableEvent, BillTextIngestedEvent}
import repcheck.pipeline.models.metadata.ProcessingResult
import repcheck.shared.models.congress.dos.bill.{BillDO, BillTextVersionDO, RawBillTextDO}

class BillTextProcessorSpec extends AnyFlatSpec with Matchers with MockitoSugar {

  private val testXa: Transactor[IO] = Transactor.fromDriverManager[IO](
    driver = "org.h2.Driver",
    url = "jdbc:h2:mem:test-text-processor;DB_CLOSE_DELAY=-1",
    user = "",
    password = "",
    logHandler = None,
  )

  private val correlationId = UUID.randomUUID()
  private val testDbBillId  = 42L

  private val testEmbeddingConfig: EmbeddingConfig = EmbeddingConfig(
    baseUrl = "http://localhost:11434",
    modelName = "qwen3-embedding",
    dimensions = 4,
    timeoutSeconds = 5,
    maxChunkChars = 30000,
  )

  private case class TestFixture(
    downloader: BillTextDownloader[IO],
    billRepository: BillRepository[ConnectionIO],
    textVersionRepository: BillTextVersionRepository[ConnectionIO],
    rawBillTextRepository: RawBillTextRepository[ConnectionIO],
    embeddingService: EmbeddingService[IO],
    eventPublisher: IngestionEventPublisher[IO],
    logger: PipelineLogger[IO],
  ) {

    def processor: BillTextProcessor[IO] =
      new BillTextProcessor[IO](
        downloader = downloader,
        billRepository = billRepository,
        textVersionRepository = textVersionRepository,
        rawBillTextRepository = rawBillTextRepository,
        embeddingService = embeddingService,
        embeddingConfig = testEmbeddingConfig,
        eventPublisher = eventPublisher,
        xa = testXa,
        logger = logger,
      )

  }

  private def createFixture(): TestFixture = {
    val loggerMock = mock[PipelineLogger[IO]]
    when(loggerMock.info(any[LogContext], anyString())).thenReturn(IO.unit)
    when(loggerMock.warn(any[LogContext], anyString())).thenReturn(IO.unit)
    when(loggerMock.error(any[LogContext], anyString(), any[Option[Throwable]])).thenReturn(IO.unit)
    when(loggerMock.debug(any[LogContext], anyString())).thenReturn(IO.unit)

    val rawRepoMock = mock[RawBillTextRepository[ConnectionIO]]
    when(rawRepoMock.replaceAll(any[Long], any[List[RawBillTextDO]])).thenReturn(doobie.free.connection.unit)

    TestFixture(
      downloader = mock[BillTextDownloader[IO]],
      billRepository = mock[BillRepository[ConnectionIO]],
      textVersionRepository = mock[BillTextVersionRepository[ConnectionIO]],
      rawBillTextRepository = rawRepoMock,
      embeddingService = mock[EmbeddingService[IO]],
      eventPublisher = mock[IngestionEventPublisher[IO]],
      logger = loggerMock,
    )
  }

  private def makeEvent(
    naturalKey: String = "118-HR-1",
    congress: Int = 118,
    textUrl: String = "https://api.congress.gov/v3/bill/118/hr/1/text/ih",
    textFormat: String = "Formatted Text",
    versionCode: String = "ih",
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

  private def stubBillLookup(f: TestFixture, billId: String = "118-HR-1", dbId: Long = testDbBillId): Unit = {
    val billDO = mock[BillDO]
    when(billDO.billId).thenReturn(dbId)
    val _ = when(f.billRepository.findByBillId(billId)).thenReturn(doobie.free.connection.pure(Some(billDO)))
  }

  private def stubSuccessfulFlow(
    f: TestFixture,
    content: String = "Bill text content here",
    billId: String = "118-HR-1",
  ): Unit = {
    stubBillLookup(f, billId)
    when(f.downloader.download(anyString(), anyString(), any[UUID])).thenReturn(IO.pure(content))
    when(f.embeddingService.generateEmbedding(anyString())).thenReturn(IO.pure(None))
    when(f.textVersionRepository.storeAndUpdateBill(any[BillTextVersionDO])).thenReturn(doobie.free.connection.pure(1L))
    val _ = when(f.eventPublisher.billTextIngested(any[BillTextIngestedEvent], any[UUID]))
      .thenReturn(IO.pure("msg-id-123"))
  }

  "processEvent" should "successfully process event end-to-end (download, store, publish)" in {
    val f     = createFixture()
    val event = makeEvent()
    stubSuccessfulFlow(f)

    val result = f.processor.processEvent(event, correlationId).unsafeRunSync()

    val _ = result.isSucceeded shouldBe true
    val _ = result.entityId shouldBe "118-HR-1"
    result shouldBe a[ProcessingResult.Succeeded]
  }

  it should "return Failed when bill not found in DB" in {
    val f     = createFixture()
    val event = makeEvent(naturalKey = "999-HR-0")
    when(f.billRepository.findByBillId("999-HR-0"))
      .thenReturn(doobie.free.connection.pure(Option.empty[BillDO]))

    val result = f.processor.processEvent(event, correlationId).unsafeRunSync()

    val _ = result.isFailed shouldBe true
    val _ = result.entityId shouldBe "999-HR-0"
    result match {
      case ProcessingResult.Failed(_, _, errorClass) => errorClass shouldBe "Systemic"
      case other                                     => fail(s"Expected Failed but got $other")
    }
  }

  it should "return Failed when download fails" in {
    val f     = createFixture()
    val event = makeEvent()
    stubBillLookup(f)
    when(f.downloader.download(anyString(), anyString(), any[UUID]))
      .thenReturn(
        IO.raiseError(TextDownloadFailed(event.textUrl, event.textFormat, "HTTP 404"))
      )

    val result = f.processor.processEvent(event, correlationId).unsafeRunSync()

    val _ = result.isFailed shouldBe true
    val _ = result.entityId shouldBe "118-HR-1"
    val _ = verify(f.textVersionRepository, never()).storeAndUpdateBill(any[BillTextVersionDO])
    verify(f.eventPublisher, never()).billTextIngested(any[BillTextIngestedEvent], any[UUID])
  }

  it should "return Failed when DB store fails" in {
    val f     = createFixture()
    val event = makeEvent()
    stubBillLookup(f)
    when(f.downloader.download(anyString(), anyString(), any[UUID])).thenReturn(IO.pure("some content"))
    when(f.embeddingService.generateEmbedding(anyString())).thenReturn(IO.pure(None))
    when(f.textVersionRepository.storeAndUpdateBill(any[BillTextVersionDO]))
      .thenReturn(doobie.free.connection.raiseError(new RuntimeException("DB connection lost")))

    val result = f.processor.processEvent(event, correlationId).unsafeRunSync()

    val _ = result.isFailed shouldBe true
    result.entityId shouldBe "118-HR-1"
  }

  it should "return Failed when event publish fails after successful store" in {
    val f     = createFixture()
    val event = makeEvent()
    stubBillLookup(f)
    when(f.downloader.download(anyString(), anyString(), any[UUID])).thenReturn(IO.pure("some content"))
    when(f.embeddingService.generateEmbedding(anyString())).thenReturn(IO.pure(None))
    when(f.textVersionRepository.storeAndUpdateBill(any[BillTextVersionDO])).thenReturn(doobie.free.connection.pure(1L))
    when(f.eventPublisher.billTextIngested(any[BillTextIngestedEvent], any[UUID]))
      .thenReturn(IO.raiseError(new RuntimeException("Pub/Sub unavailable")))

    val result = f.processor.processEvent(event, correlationId).unsafeRunSync()

    val _ = result.isFailed shouldBe true
    result.entityId shouldBe "118-HR-1"
  }

  it should "populate BillTextVersionDO with correct fields including DB bill ID" in {
    val f = createFixture()
    val event = makeEvent(
      naturalKey = "118-S-42",
      textUrl = "https://api.congress.gov/v3/bill/118/s/42/text/enr",
      textFormat = "Formatted XML",
      versionCode = "enr",
    )
    stubSuccessfulFlow(f, content = "Enrolled bill text", billId = "118-S-42")

    val _ = f.processor.processEvent(event, correlationId).unsafeRunSync()

    val captor = ArgumentCaptor.forClass(classOf[BillTextVersionDO])
    val _      = verify(f.textVersionRepository, times(1)).storeAndUpdateBill(captor.capture())
    val stored = captor.getValue

    val _ = stored.billId shouldBe testDbBillId
    val _ = stored.versionCode shouldBe "enr"
    val _ = stored.versionType shouldBe "Formatted XML"
    val _ = stored.url shouldBe Some("https://api.congress.gov/v3/bill/118/s/42/text/enr")
    val _ = stored.fetchedAt.toString should not be empty

    // Content now lives in raw_bill_text chunk rows (P6.H4c refactor); assert it was forwarded there.
    val rawCaptor = ArgumentCaptor.forClass(classOf[List[RawBillTextDO]])
    val _         = verify(f.rawBillTextRepository, times(1)).replaceAll(any[Long], rawCaptor.capture())
    rawCaptor.getValue.map(_.content).mkString shouldBe "Enrolled bill text"
  }

  it should "include embedding on raw_bill_text chunk rows when embedding service returns one" in {
    val f         = createFixture()
    val event     = makeEvent()
    val embedding = Array(0.1f, 0.2f, 0.3f)
    stubBillLookup(f)
    when(f.downloader.download(anyString(), anyString(), any[UUID])).thenReturn(IO.pure("some content"))
    when(f.embeddingService.generateEmbedding("some content")).thenReturn(IO.pure(Some(embedding)))
    when(f.textVersionRepository.storeAndUpdateBill(any[BillTextVersionDO])).thenReturn(doobie.free.connection.pure(1L))
    val _ = when(f.eventPublisher.billTextIngested(any[BillTextIngestedEvent], any[UUID]))
      .thenReturn(IO.pure("msg-id-123"))

    val _ = f.processor.processEvent(event, correlationId).unsafeRunSync()

    val rawCaptor = ArgumentCaptor.forClass(classOf[List[RawBillTextDO]])
    val _         = verify(f.rawBillTextRepository, times(1)).replaceAll(any[Long], rawCaptor.capture())
    val chunks    = rawCaptor.getValue

    val _ = chunks should not be empty
    chunks.headOption.flatMap(_.embedding).map(_.toList) shouldBe Some(embedding.toList)
  }

  it should "set embedding to None on raw_bill_text chunk rows when embedding service returns None" in {
    val f     = createFixture()
    val event = makeEvent()
    stubSuccessfulFlow(f)

    val _ = f.processor.processEvent(event, correlationId).unsafeRunSync()

    val rawCaptor = ArgumentCaptor.forClass(classOf[List[RawBillTextDO]])
    val _         = verify(f.rawBillTextRepository, times(1)).replaceAll(any[Long], rawCaptor.capture())
    val chunks    = rawCaptor.getValue

    val _ = chunks should not be empty
    chunks.headOption.flatMap(_.embedding) shouldBe None
  }

  it should "populate BillTextIngestedEvent with correct fields" in {
    val f = createFixture()
    val event = makeEvent(
      naturalKey = "118-HR-99",
      congress = 118,
      versionCode = "rh",
      previousVersionCode = Some("ih"),
    )
    stubSuccessfulFlow(f, billId = "118-HR-99")

    val _ = f.processor.processEvent(event, correlationId).unsafeRunSync()

    val captor    = ArgumentCaptor.forClass(classOf[BillTextIngestedEvent])
    val _         = verify(f.eventPublisher, times(1)).billTextIngested(captor.capture(), any[UUID])
    val published = captor.getValue

    val _ = published.naturalKey shouldBe "118-HR-99"
    val _ = published.congress shouldBe 118
    val _ = published.versionCode shouldBe "rh"
    published.previousVersionCode shouldBe Some("ih")
  }

  it should "classify IO exceptions as Transient" in {
    val f     = createFixture()
    val event = makeEvent()
    stubBillLookup(f)
    when(f.downloader.download(anyString(), anyString(), any[UUID]))
      .thenReturn(IO.raiseError(new java.io.IOException("network error")))

    val result = f.processor.processEvent(event, correlationId).unsafeRunSync()

    val _ = result.isFailed shouldBe true
    result match {
      case ProcessingResult.Failed(_, _, errorClass) => errorClass shouldBe "Transient"
      case other                                     => fail(s"Expected Failed but got $other")
    }
  }

  it should "classify BillTextProcessingFailed as Systemic" in {
    val f     = createFixture()
    val event = makeEvent()
    stubBillLookup(f)
    when(f.downloader.download(anyString(), anyString(), any[UUID]))
      .thenReturn(IO.raiseError(BillTextProcessingFailed("118-HR-1", "invalid format")))

    val result = f.processor.processEvent(event, correlationId).unsafeRunSync()

    val _ = result.isFailed shouldBe true
    result match {
      case ProcessingResult.Failed(_, _, errorClass) => errorClass shouldBe "Systemic"
      case other                                     => fail(s"Expected Failed but got $other")
    }
  }

  it should "classify BillNotFoundForText as Systemic" in {
    val f     = createFixture()
    val event = makeEvent(naturalKey = "999-HR-0")
    when(f.billRepository.findByBillId("999-HR-0"))
      .thenReturn(doobie.free.connection.pure(Option.empty[BillDO]))

    val result = f.processor.processEvent(event, correlationId).unsafeRunSync()

    val _ = result.isFailed shouldBe true
    result match {
      case ProcessingResult.Failed(_, _, errorClass) => errorClass shouldBe "Systemic"
      case other                                     => fail(s"Expected Failed but got $other")
    }
  }

  it should "not publish event when download fails" in {
    val f     = createFixture()
    val event = makeEvent()
    stubBillLookup(f)
    when(f.downloader.download(anyString(), anyString(), any[UUID]))
      .thenReturn(IO.raiseError(new RuntimeException("download failed")))

    val _ = f.processor.processEvent(event, correlationId).unsafeRunSync()
    verify(f.eventPublisher, never()).billTextIngested(any[BillTextIngestedEvent], any[UUID])
  }

  it should "log error when processing fails" in {
    val f     = createFixture()
    val event = makeEvent()
    stubBillLookup(f)
    when(f.downloader.download(anyString(), anyString(), any[UUID]))
      .thenReturn(IO.raiseError(new RuntimeException("test failure")))

    val _ = f.processor.processEvent(event, correlationId).unsafeRunSync()
    verify(f.logger, times(1)).error(any[LogContext], anyString(), any[Option[Throwable]])
  }

  it should "set eventEmitted to true on success" in {
    val f     = createFixture()
    val event = makeEvent()
    stubSuccessfulFlow(f)

    val result = f.processor.processEvent(event, correlationId).unsafeRunSync()

    result match {
      case ProcessingResult.Succeeded(_, eventEmitted) => eventEmitted shouldBe true
      case other                                       => fail(s"Expected Succeeded but got $other")
    }
  }

  it should "return Failed when embedding generation fails" in {
    val f     = createFixture()
    val event = makeEvent()
    stubBillLookup(f)
    when(f.downloader.download(anyString(), anyString(), any[UUID])).thenReturn(IO.pure("some content"))
    when(f.embeddingService.generateEmbedding(anyString()))
      .thenReturn(IO.raiseError(new RuntimeException("embedding model unavailable")))

    val result = f.processor.processEvent(event, correlationId).unsafeRunSync()

    val _ = result.isFailed shouldBe true
    result.entityId shouldBe "118-HR-1"
  }

  it should "classify EmbeddingGenerationFailed as Transient" in {
    val f     = createFixture()
    val event = makeEvent()
    stubBillLookup(f)
    when(f.downloader.download(anyString(), anyString(), any[UUID])).thenReturn(IO.pure("some content"))
    when(f.embeddingService.generateEmbedding(anyString()))
      .thenReturn(IO.raiseError(EmbeddingGenerationFailed("model unavailable", 12)))

    val result = f.processor.processEvent(event, correlationId).unsafeRunSync()

    val _ = result.isFailed shouldBe true
    result match {
      case ProcessingResult.Failed(_, _, errorClass) => errorClass shouldBe "Transient"
      case other                                     => fail(s"Expected Failed but got $other")
    }
  }

  it should "classify SocketTimeoutException as Transient" in {
    val f     = createFixture()
    val event = makeEvent()
    stubBillLookup(f)
    when(f.downloader.download(anyString(), anyString(), any[UUID]))
      .thenReturn(IO.raiseError(new java.net.SocketTimeoutException("timeout")))

    val result = f.processor.processEvent(event, correlationId).unsafeRunSync()

    val _ = result.isFailed shouldBe true
    result match {
      case ProcessingResult.Failed(_, _, errorClass) => errorClass shouldBe "Transient"
      case other                                     => fail(s"Expected Failed but got $other")
    }
  }

  it should "classify ConnectException as Transient" in {
    val f     = createFixture()
    val event = makeEvent()
    stubBillLookup(f)
    when(f.downloader.download(anyString(), anyString(), any[UUID]))
      .thenReturn(IO.raiseError(new java.net.ConnectException("connection refused")))

    val result = f.processor.processEvent(event, correlationId).unsafeRunSync()

    val _ = result.isFailed shouldBe true
    result match {
      case ProcessingResult.Failed(_, _, errorClass) => errorClass shouldBe "Transient"
      case other                                     => fail(s"Expected Failed but got $other")
    }
  }

  it should "classify SQLTransientException as Transient" in {
    val f     = createFixture()
    val event = makeEvent()
    stubBillLookup(f)
    when(f.downloader.download(anyString(), anyString(), any[UUID]))
      .thenReturn(IO.raiseError(new java.sql.SQLTransientConnectionException("db connection lost")))

    val result = f.processor.processEvent(event, correlationId).unsafeRunSync()

    val _ = result.isFailed shouldBe true
    result match {
      case ProcessingResult.Failed(_, _, errorClass) => errorClass shouldBe "Transient"
      case other                                     => fail(s"Expected Failed but got $other")
    }
  }

  it should "strip null bytes from content before storing" in {
    val f                = createFixture()
    val event            = makeEvent()
    val contentWithNulls = "Bill\u0000text\u0000with\u0000nulls"
    stubBillLookup(f)
    when(f.downloader.download(anyString(), anyString(), any[UUID])).thenReturn(IO.pure(contentWithNulls))
    when(f.embeddingService.generateEmbedding(anyString())).thenReturn(IO.pure(None))
    when(f.textVersionRepository.storeAndUpdateBill(any[BillTextVersionDO])).thenReturn(doobie.free.connection.pure(1L))
    val _ = when(f.eventPublisher.billTextIngested(any[BillTextIngestedEvent], any[UUID]))
      .thenReturn(IO.pure("msg-id-123"))

    val _ = f.processor.processEvent(event, correlationId).unsafeRunSync()

    val rawCaptor = ArgumentCaptor.forClass(classOf[List[RawBillTextDO]])
    val _         = verify(f.rawBillTextRepository, times(1)).replaceAll(any[Long], rawCaptor.capture())
    rawCaptor.getValue.map(_.content).mkString shouldBe "Billtextwithnulls"
  }

  it should "classify unknown exceptions as Systemic by default" in {
    val f     = createFixture()
    val event = makeEvent()
    stubBillLookup(f)
    when(f.downloader.download(anyString(), anyString(), any[UUID]))
      .thenReturn(IO.raiseError(new RuntimeException("unexpected error")))

    val result = f.processor.processEvent(event, correlationId).unsafeRunSync()

    val _ = result.isFailed shouldBe true
    result match {
      case ProcessingResult.Failed(_, _, errorClass) => errorClass shouldBe "Systemic"
      case other                                     => fail(s"Expected Failed but got $other")
    }
  }

}
