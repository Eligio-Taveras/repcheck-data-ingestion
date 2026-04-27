package repcheck.ingestion.bills.text.pipeline

import java.time.Instant
import java.util.UUID
import java.util.concurrent.atomic.AtomicReference

import cats.effect.IO
import cats.effect.unsafe.implicits.global

import fs2.Stream

import doobie._

import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.{any, anyLong, anyString}
import org.mockito.Mockito.{never, times, verify, when}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar
import repcheck.ingestion.bills.common.persistence.{BillRepository, BillTextVersionRepository}
import repcheck.ingestion.bills.text.download.BillTextDownloader
import repcheck.ingestion.bills.text.embedding.{
  EmbeddingConfig,
  EmbeddingContextLengthExceeded,
  EmbeddingGenerationFailed,
  EmbeddingService,
}
import repcheck.ingestion.bills.text.errors.{BillTextProcessingFailed, TextDownloadFailed}
import repcheck.ingestion.bills.text.persistence.RawBillTextRepository
import repcheck.ingestion.common.events.IngestionEventPublisher
import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}
import repcheck.pipeline.models.events.{BillTextAvailableEvent, BillTextIngestedEvent}
import repcheck.pipeline.models.metadata.ProcessingResult
import repcheck.shared.models.congress.dos.bill.{BillDO, BillTextVersionDO, RawBillTextDO}

/**
 * Unit specs for [[BillTextProcessor]] under the streaming-to-temp-file refactor (Phase 2 of the bill-text-10mb plan).
 * Mocks the new collaborators:
 *
 *   - `downloader.downloadToTempFile(...)` returns a `Resource[F, Path]` that yields a dummy temp file path; tests
 *     don't actually write a file because the injected `extractText` function returns canned content via
 *     `TestFixture.contentResponseRef`.
 *   - `extractText` is the new constructor-injected function on `BillTextProcessor` (production wiring uses
 *     [[repcheck.ingestion.bills.text.extraction.BillTextExtractor.extract]]). Tests inject a stub via the fixture.
 *   - `rawBillTextRepository.deleteByVersionId` and `insertOne` replace the old `replaceAll` bulk path; the processor
 *     now persists chunks one at a time after the version row is committed.
 *   - `textVersionRepository.markFetched` is the new completion marker — flips `fetched_at = NOW()` after all chunks
 *     INSERT successfully.
 */
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
    modelName = "bill-text-embedding",
    dimensions = 4,
    timeoutSeconds = 5,
    maxChunkChars = 30000,
    embedBatchSize = 10,
  )

  private case class TestFixture(
    downloader: BillTextDownloader[IO],
    billRepository: BillRepository[ConnectionIO],
    textVersionRepository: BillTextVersionRepository[ConnectionIO],
    rawBillTextRepository: RawBillTextRepository[ConnectionIO],
    embeddingService: EmbeddingService[IO],
    eventPublisher: IngestionEventPublisher[IO],
    logger: PipelineLogger[IO],
    contentResponseRef: AtomicReference[IO[String]],
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
        extractText = (_, _) => Stream.eval(contentResponseRef.get()).flatMap(Stream.emit),
      )

    /**
     * Stub the success path: `streamBody` yields an empty byte stream (the injected `extractText` stub ignores it and
     * emits `content` directly via `contentResponseRef`). Caller is still responsible for stubbing
     * `billRepository.findByBillId`, embedding service responses, etc.
     */
    def stubSuccessfulDownload(content: String): Unit = {
      contentResponseRef.set(IO.pure(content))
      val _ = when(downloader.streamBody(anyString(), anyString(), any[UUID]))
        .thenReturn(Stream.empty.covary[IO])
    }

    /**
     * Stub a failure during the download phase (the byte stream raises). To make the stubbed `extractText` (which
     * ignores its byte-stream argument and instead emits from `contentResponseRef`) actually surface the error, we also
     * flip `contentResponseRef` to raise the same error. In production the byte-stream-level error and the
     * extractor-level error are the same propagation chain, so this dual-stub matches reality.
     */
    def stubDownloadFailure(error: Throwable): Unit = {
      contentResponseRef.set(IO.raiseError[String](error))
      val _ = when(downloader.streamBody(anyString(), anyString(), any[UUID]))
        .thenReturn(Stream.raiseError[IO](error))
    }

    /**
     * Stub a failure during the extraction phase (download succeeds but the streaming extractor raises). The injected
     * `extractText` stub flatMaps the contentResponseRef IO; setting it to `IO.raiseError` makes the resulting Stream
     * fail when pulled.
     */
    def stubExtractFailure(error: Throwable): Unit = {
      contentResponseRef.set(IO.raiseError[String](error))
      val _ = when(downloader.streamBody(anyString(), anyString(), any[UUID]))
        .thenReturn(Stream.empty.covary[IO])
    }

  }

  private def createFixture(): TestFixture = {
    val loggerMock = mock[PipelineLogger[IO]]
    when(loggerMock.info(any[LogContext], anyString())).thenReturn(IO.unit)
    when(loggerMock.warn(any[LogContext], anyString())).thenReturn(IO.unit)
    when(loggerMock.error(any[LogContext], anyString(), any[Option[Throwable]])).thenReturn(IO.unit)
    when(loggerMock.debug(any[LogContext], anyString())).thenReturn(IO.unit)

    // New repo methods used by the streaming-INSERT flow: deleteByVersionId clears orphan chunks before insert,
    // insertOne adds each chunk, and replaceAll is retained for backward-compat callers (none post-Phase 2).
    val rawRepoMock = mock[RawBillTextRepository[ConnectionIO]]
    when(rawRepoMock.deleteByVersionId(anyLong())).thenReturn(doobie.free.connection.unit)
    when(rawRepoMock.insertOne(any[RawBillTextDO])).thenReturn(doobie.free.connection.unit)
    when(rawRepoMock.replaceAll(any[Long], any[List[RawBillTextDO]])).thenReturn(doobie.free.connection.unit)

    // Default: bill_text_versions has no row for the (billId, versionCode) pair, so the new `isAlreadyProcessed`
    // skip-check returns false and processing proceeds. Tests that exercise the "already processed" skip path
    // override this stub with a list containing a row whose `fetchedAt` is `Some(...)`.
    val textVersionRepoMock = mock[BillTextVersionRepository[ConnectionIO]]
    when(textVersionRepoMock.findByBillId(any[Long]))
      .thenReturn(doobie.free.connection.pure(List.empty[BillTextVersionDO]))
    when(textVersionRepoMock.markFetched(anyLong(), any[Instant])).thenReturn(doobie.free.connection.unit)

    TestFixture(
      downloader = mock[BillTextDownloader[IO]],
      billRepository = mock[BillRepository[ConnectionIO]],
      textVersionRepository = textVersionRepoMock,
      rawBillTextRepository = rawRepoMock,
      embeddingService = mock[EmbeddingService[IO]],
      eventPublisher = mock[IngestionEventPublisher[IO]],
      logger = loggerMock,
      contentResponseRef = new AtomicReference[IO[String]](IO.pure("")),
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
    f.stubSuccessfulDownload(content)
    when(f.embeddingService.generateEmbeddings(any[List[String]]()))
      .thenAnswer { (invocation: org.mockito.invocation.InvocationOnMock) =>
        val texts = invocation.getArgument[List[String]](0)
        IO.pure(List.fill(texts.size)(None))
      }
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
    f.stubDownloadFailure(TextDownloadFailed(event.textUrl, event.textFormat, "HTTP 404"))
    when(f.textVersionRepository.storeAndUpdateBill(any[BillTextVersionDO])).thenReturn(doobie.free.connection.pure(1L))

    val result = f.processor.processEvent(event, correlationId).unsafeRunSync()

    val _ = result.isFailed shouldBe true
    val _ = result.entityId shouldBe "118-HR-1"
    // Download fails AFTER the version row is inserted (so the next pipeline tick can detect the partial state via
    // `fetched_at IS NULL`), but BEFORE markFetched / event publish run.
    val _ = verify(f.textVersionRepository, never()).markFetched(anyLong(), any[Instant])
    verify(f.eventPublisher, never()).billTextIngested(any[BillTextIngestedEvent], any[UUID])
  }

  it should "return Failed when DB store fails" in {
    val f     = createFixture()
    val event = makeEvent()
    stubBillLookup(f)
    f.stubSuccessfulDownload("some content")
    when(f.embeddingService.generateEmbeddings(any[List[String]]()))
      .thenAnswer { (invocation: org.mockito.invocation.InvocationOnMock) =>
        val texts = invocation.getArgument[List[String]](0)
        IO.pure(List.fill(texts.size)(None))
      }
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
    f.stubSuccessfulDownload("some content")
    when(f.embeddingService.generateEmbeddings(any[List[String]]()))
      .thenAnswer { (invocation: org.mockito.invocation.InvocationOnMock) =>
        val texts = invocation.getArgument[List[String]](0)
        IO.pure(List.fill(texts.size)(None))
      }
    when(f.textVersionRepository.storeAndUpdateBill(any[BillTextVersionDO])).thenReturn(doobie.free.connection.pure(1L))
    when(f.eventPublisher.billTextIngested(any[BillTextIngestedEvent], any[UUID]))
      .thenReturn(IO.raiseError(new RuntimeException("Pub/Sub unavailable")))

    val result = f.processor.processEvent(event, correlationId).unsafeRunSync()

    val _ = result.isFailed shouldBe true
    result.entityId shouldBe "118-HR-1"
  }

  it should "populate BillTextVersionDO with correct fields including DB bill ID and fetched_at = None on initial insert" in {
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
    // Phase 2 streaming flow inserts the version row with fetched_at = NULL; it gets flipped to NOW() by markFetched
    // at the end of successful chunk persistence.
    val _ = stored.fetchedAt shouldBe None

    // Per-chunk INSERTs replaced bulk replaceAll. Capture the chunk argument to insertOne and assert content roundtrip.
    val rawCaptor = ArgumentCaptor.forClass(classOf[RawBillTextDO])
    val _         = verify(f.rawBillTextRepository, times(1)).insertOne(rawCaptor.capture())
    rawCaptor.getValue.content shouldBe "Enrolled bill text"
  }

  it should "include embedding on raw_bill_text chunk rows when embedding service returns one" in {
    val f         = createFixture()
    val event     = makeEvent()
    val embedding = Array(0.1f, 0.2f, 0.3f)
    stubBillLookup(f)
    f.stubSuccessfulDownload("some content")
    when(f.embeddingService.generateEmbeddings(List("some content"))).thenReturn(IO.pure(List(Some(embedding))))
    when(f.textVersionRepository.storeAndUpdateBill(any[BillTextVersionDO])).thenReturn(doobie.free.connection.pure(1L))
    val _ = when(f.eventPublisher.billTextIngested(any[BillTextIngestedEvent], any[UUID]))
      .thenReturn(IO.pure("msg-id-123"))

    val _ = f.processor.processEvent(event, correlationId).unsafeRunSync()

    val rawCaptor = ArgumentCaptor.forClass(classOf[RawBillTextDO])
    val _         = verify(f.rawBillTextRepository, times(1)).insertOne(rawCaptor.capture())
    val chunk     = rawCaptor.getValue

    chunk.embedding.map(_.toList) shouldBe Some(embedding.toList)
  }

  it should "set embedding to None on raw_bill_text chunk rows when embedding service returns None" in {
    val f     = createFixture()
    val event = makeEvent()
    stubSuccessfulFlow(f)

    val _ = f.processor.processEvent(event, correlationId).unsafeRunSync()

    val rawCaptor = ArgumentCaptor.forClass(classOf[RawBillTextDO])
    val _         = verify(f.rawBillTextRepository, times(1)).insertOne(rawCaptor.capture())
    rawCaptor.getValue.embedding shouldBe None
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
    f.stubDownloadFailure(new java.io.IOException("network error"))
    when(f.textVersionRepository.storeAndUpdateBill(any[BillTextVersionDO])).thenReturn(doobie.free.connection.pure(1L))

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
    f.stubDownloadFailure(BillTextProcessingFailed("118-HR-1", "invalid format"))
    when(f.textVersionRepository.storeAndUpdateBill(any[BillTextVersionDO])).thenReturn(doobie.free.connection.pure(1L))

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
    f.stubDownloadFailure(new RuntimeException("download failed"))
    when(f.textVersionRepository.storeAndUpdateBill(any[BillTextVersionDO])).thenReturn(doobie.free.connection.pure(1L))

    val _ = f.processor.processEvent(event, correlationId).unsafeRunSync()
    verify(f.eventPublisher, never()).billTextIngested(any[BillTextIngestedEvent], any[UUID])
  }

  it should "log error when processing fails" in {
    val f     = createFixture()
    val event = makeEvent()
    stubBillLookup(f)
    f.stubDownloadFailure(new RuntimeException("test failure"))
    when(f.textVersionRepository.storeAndUpdateBill(any[BillTextVersionDO])).thenReturn(doobie.free.connection.pure(1L))

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
    f.stubSuccessfulDownload("some content")
    when(f.embeddingService.generateEmbeddings(any[List[String]]()))
      .thenReturn(IO.raiseError(new RuntimeException("embedding model unavailable")))
    when(f.textVersionRepository.storeAndUpdateBill(any[BillTextVersionDO])).thenReturn(doobie.free.connection.pure(1L))

    val result = f.processor.processEvent(event, correlationId).unsafeRunSync()

    val _ = result.isFailed shouldBe true
    result.entityId shouldBe "118-HR-1"
  }

  it should "classify EmbeddingGenerationFailed as Transient" in {
    val f     = createFixture()
    val event = makeEvent()
    stubBillLookup(f)
    f.stubSuccessfulDownload("some content")
    when(f.embeddingService.generateEmbeddings(any[List[String]]()))
      .thenReturn(IO.raiseError(EmbeddingGenerationFailed("model unavailable", 12)))
    when(f.textVersionRepository.storeAndUpdateBill(any[BillTextVersionDO])).thenReturn(doobie.free.connection.pure(1L))

    val result = f.processor.processEvent(event, correlationId).unsafeRunSync()

    val _ = result.isFailed shouldBe true
    result match {
      case ProcessingResult.Failed(_, _, errorClass) => errorClass shouldBe "Transient"
      case other                                     => fail(s"Expected Failed but got $other")
    }
  }

  it should "classify EmbeddingContextLengthExceeded as Systemic" in {
    val f     = createFixture()
    val event = makeEvent()
    stubBillLookup(f)
    f.stubSuccessfulDownload("some content")
    when(f.embeddingService.generateEmbeddings(any[List[String]]()))
      .thenReturn(
        IO.raiseError(
          EmbeddingContextLengthExceeded("input length exceeds context length", 30000)
        )
      )
    when(f.textVersionRepository.storeAndUpdateBill(any[BillTextVersionDO])).thenReturn(doobie.free.connection.pure(1L))

    val result = f.processor.processEvent(event, correlationId).unsafeRunSync()

    val _ = result.isFailed shouldBe true
    result match {
      case ProcessingResult.Failed(_, _, errorClass) => errorClass shouldBe "Systemic"
      case other                                     => fail(s"Expected Failed but got $other")
    }
  }

  it should "classify SocketTimeoutException as Transient" in {
    val f     = createFixture()
    val event = makeEvent()
    stubBillLookup(f)
    f.stubDownloadFailure(new java.net.SocketTimeoutException("timeout"))
    when(f.textVersionRepository.storeAndUpdateBill(any[BillTextVersionDO])).thenReturn(doobie.free.connection.pure(1L))

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
    f.stubDownloadFailure(new java.net.ConnectException("connection refused"))
    when(f.textVersionRepository.storeAndUpdateBill(any[BillTextVersionDO])).thenReturn(doobie.free.connection.pure(1L))

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
    f.stubDownloadFailure(new java.sql.SQLTransientConnectionException("db connection lost"))
    when(f.textVersionRepository.storeAndUpdateBill(any[BillTextVersionDO])).thenReturn(doobie.free.connection.pure(1L))

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
    val contentWithNulls = "Bill text with nulls"
    stubBillLookup(f)
    f.stubSuccessfulDownload(contentWithNulls)
    when(f.embeddingService.generateEmbeddings(any[List[String]]()))
      .thenAnswer { (invocation: org.mockito.invocation.InvocationOnMock) =>
        val texts = invocation.getArgument[List[String]](0)
        IO.pure(List.fill(texts.size)(None))
      }
    when(f.textVersionRepository.storeAndUpdateBill(any[BillTextVersionDO])).thenReturn(doobie.free.connection.pure(1L))
    val _ = when(f.eventPublisher.billTextIngested(any[BillTextIngestedEvent], any[UUID]))
      .thenReturn(IO.pure("msg-id-123"))

    val _ = f.processor.processEvent(event, correlationId).unsafeRunSync()

    val rawCaptor = ArgumentCaptor.forClass(classOf[RawBillTextDO])
    val _         = verify(f.rawBillTextRepository, times(1)).insertOne(rawCaptor.capture())
    rawCaptor.getValue.content shouldBe "Billtextwithnulls"
  }

  it should "classify unknown exceptions as Systemic by default" in {
    val f     = createFixture()
    val event = makeEvent()
    stubBillLookup(f)
    f.stubDownloadFailure(new RuntimeException("unexpected error"))
    when(f.textVersionRepository.storeAndUpdateBill(any[BillTextVersionDO])).thenReturn(doobie.free.connection.pure(1L))

    val result = f.processor.processEvent(event, correlationId).unsafeRunSync()

    val _ = result.isFailed shouldBe true
    result match {
      case ProcessingResult.Failed(_, _, errorClass) => errorClass shouldBe "Systemic"
      case other                                     => fail(s"Expected Failed but got $other")
    }
  }

  it should "call markFetched on the version repo after successful chunk persistence" in {
    val f     = createFixture()
    val event = makeEvent()
    stubSuccessfulFlow(f)

    val _ = f.processor.processEvent(event, correlationId).unsafeRunSync()

    verify(f.textVersionRepository, times(1)).markFetched(anyLong(), any[Instant])
  }

  it should "delete orphan chunks before streaming new chunks (idempotent retry)" in {
    val f     = createFixture()
    val event = makeEvent()
    stubSuccessfulFlow(f)

    val _ = f.processor.processEvent(event, correlationId).unsafeRunSync()

    // The deleteByVersionId is the cleanup-before-stream step; without it a retry would hit the (version_id,
    // chunk_index) unique constraint on duplicate chunk_index values left from the previous attempt.
    verify(f.rawBillTextRepository, times(1)).deleteByVersionId(anyLong())
  }

  // ---------------------------------------------------------------------------
  // isAlreadyProcessed skip path — exercises the TRUE branch of the early skip-check.
  // Phase 2 update: the skip-check now also requires `fetched_at` to be Some(...) so it only counts COMPLETE
  // versions as "already processed". A row with fetched_at = None is treated as an in-flight or crashed-mid-flight
  // run that needs re-processing.
  // ---------------------------------------------------------------------------

  "isAlreadyProcessed skip-check" should "return Skipped(\"already-processed\") when bill_text_versions row matches the event versionCode AND is fetched" in {
    val f     = createFixture()
    val event = makeEvent(naturalKey = "118-HR-1", versionCode = "ih")
    stubBillLookup(f)

    // Override the default empty stub with a row whose versionCode matches AND fetched_at is set.
    val existingVersion = mock[BillTextVersionDO]
    when(existingVersion.versionCode).thenReturn("ih")
    when(existingVersion.fetchedAt).thenReturn(Some(Instant.now()))
    when(f.textVersionRepository.findByBillId(testDbBillId))
      .thenReturn(doobie.free.connection.pure(List(existingVersion)))

    val result = f.processor.processEvent(event, correlationId).unsafeRunSync()

    val _ = result.isSkipped shouldBe true
    result match {
      case ProcessingResult.Skipped(entityId, reason) =>
        val _ = entityId shouldBe "118-HR-1"
        reason shouldBe "already-processed"
      case other => fail(s"Expected Skipped but got $other")
    }
  }

  it should "skip the expensive download/embed/persist work when already processed" in {
    val f     = createFixture()
    val event = makeEvent(naturalKey = "118-HR-1", versionCode = "ih")
    stubBillLookup(f)

    val existingVersion = mock[BillTextVersionDO]
    when(existingVersion.versionCode).thenReturn("ih")
    when(existingVersion.fetchedAt).thenReturn(Some(Instant.now()))
    when(f.textVersionRepository.findByBillId(testDbBillId))
      .thenReturn(doobie.free.connection.pure(List(existingVersion)))

    val _ = f.processor.processEvent(event, correlationId).unsafeRunSync()

    // None of the heavy-lifting collaborators should have been touched.
    val _ = verify(f.downloader, never()).streamBody(anyString(), anyString(), any[UUID])
    val _ = verify(f.embeddingService, never()).generateEmbeddings(any[List[String]]())
    val _ = verify(f.textVersionRepository, never()).storeAndUpdateBill(any[BillTextVersionDO])
    val _ = verify(f.rawBillTextRepository, never()).insertOne(any[RawBillTextDO])
    verify(f.eventPublisher, never()).billTextIngested(any[BillTextIngestedEvent], any[UUID])
  }

  it should "still call processFreshBillText when bill_text_versions has rows for OTHER versionCodes" in {
    // Multiple stored versions, none matching → exists() is false → not already-processed → process fresh.
    val f     = createFixture()
    val event = makeEvent(naturalKey = "118-HR-1", versionCode = "rh")
    stubSuccessfulFlow(f)

    val ihVersion = mock[BillTextVersionDO]
    when(ihVersion.versionCode).thenReturn("ih")
    when(ihVersion.fetchedAt).thenReturn(Some(Instant.now()))
    val ehVersion = mock[BillTextVersionDO]
    when(ehVersion.versionCode).thenReturn("eh")
    when(ehVersion.fetchedAt).thenReturn(Some(Instant.now()))
    when(f.textVersionRepository.findByBillId(testDbBillId))
      .thenReturn(doobie.free.connection.pure(List(ihVersion, ehVersion)))

    val result = f.processor.processEvent(event, correlationId).unsafeRunSync()

    val _ = result.isSucceeded shouldBe true
    // Heavy path WAS exercised because no stored version matched "rh".
    val _ = verify(f.downloader, times(1)).streamBody(anyString(), anyString(), any[UUID])
    verify(f.eventPublisher, times(1)).billTextIngested(any[BillTextIngestedEvent], any[UUID])
  }

  it should "REPROCESS when the matching versionCode row exists but fetched_at is None (crashed mid-flight)" in {
    // This is the new behavior: a row with fetched_at = None signals a crashed-or-in-flight run, and
    // isAlreadyProcessed returns false so the next pipeline tick re-attempts from scratch.
    val f     = createFixture()
    val event = makeEvent(naturalKey = "118-HR-1", versionCode = "ih")
    stubSuccessfulFlow(f)

    val partialVersion = mock[BillTextVersionDO]
    when(partialVersion.versionCode).thenReturn("ih")
    when(partialVersion.fetchedAt).thenReturn(None) // Crashed mid-flight — should reprocess.
    when(f.textVersionRepository.findByBillId(testDbBillId))
      .thenReturn(doobie.free.connection.pure(List(partialVersion)))

    val result = f.processor.processEvent(event, correlationId).unsafeRunSync()

    val _ = result.isSucceeded shouldBe true
    verify(f.downloader, times(1)).streamBody(anyString(), anyString(), any[UUID])
  }

}
