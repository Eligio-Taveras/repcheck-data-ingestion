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
import org.mockito.Mockito.{doAnswer, never, times, verify, when}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar
import repcheck.ingestion.bills.common.persistence.{BillRepository, BillTextVersionRepository}
import repcheck.ingestion.bills.text.download.BillTextDownloader
import repcheck.ingestion.bills.text.embedding.{BillChunkEmbedder, BillEmbedCtx}
import repcheck.ingestion.bills.text.errors.{BillTextProcessingFailed, TextDownloadFailed}
import repcheck.ingestion.bills.text.persistence.RawBillTextRepository
import repcheck.ingestion.common.events.IngestionEventPublisher
import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}
import repcheck.ingestion.text.embedding.EmbeddingConfig
import repcheck.pipeline.models.events.{BillTextAvailableEvent, BillTextIngestedEvent}
import repcheck.pipeline.models.metadata.ProcessingResult
import repcheck.shared.models.congress.dos.bill.{BillDO, BillTextVersionDO}

/**
 * Unit specs for [[BillTextProcessor]] under the cross-bill embedder refactor (Phase 4 of the bill-text-10mb plan).
 *
 * The processor's responsibilities, post-Phase-4, are intentionally narrow — most of the per-chunk work moved into
 * [[repcheck.ingestion.bills.text.embedding.CrossBillEmbedder]] so that chunks from different bills can be batched into
 * a single Ollama call (the GPU was sitting at ~25% utilization with one-bill-per-batch). What the processor still
 * owns:
 *
 *   - skip-check vs bill_text_versions (`isAlreadyProcessed`)
 *   - insert the version row with `fetched_at = NULL`
 *   - clear orphan chunks for retry-safety
 *   - build the chunk stream (download → extract → strip-null-bytes → chunkPipe)
 *   - hand the chunk stream off to the embedder via `embedder.processChunks`
 *   - mark version `fetched` and publish the BillTextIngestedEvent on success
 *   - propagate the embedder's classified `Failed` result without re-classifying it (the embedder knows whether the
 *     failure was Transient/Systemic; the processor must not flatten that signal)
 *
 * What the processor no longer owns (and what these specs do NOT test — they live in `CrossBillEmbedderSpec`):
 *
 *   - calling `embeddingService.generateEmbeddings`
 *   - inserting individual `RawBillTextDO` rows (now batched into `insertMany` per cross-bill batch)
 *   - classifying embedding errors as Transient vs Systemic (the embedder does that)
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
    embedBatchTimeout = scala.concurrent.duration.DurationInt(1).second,
    embedQueueCapacityMultiplier = 10,
  )

  private case class TestFixture(
    downloader: BillTextDownloader[IO],
    billRepository: BillRepository[ConnectionIO],
    textVersionRepository: BillTextVersionRepository[ConnectionIO],
    rawBillTextRepository: RawBillTextRepository[ConnectionIO],
    embedder: BillChunkEmbedder[IO],
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
        embedder = embedder,
        embeddingConfig = testEmbeddingConfig,
        eventPublisher = eventPublisher,
        xa = testXa,
        logger = logger,
        // The injected `extractText` ignores the byte stream and yields whatever string is currently in the
        // contentResponseRef. Tests use this to inject content (or failures) without ever touching real bytes.
        extractText = (_, _) => Stream.eval(contentResponseRef.get()).flatMap(Stream.emit),
      )

    /** Stub the success path: byte stream is empty (extractor stub ignores it; emits whatever is in the ref). */
    def stubSuccessfulDownload(content: String): Unit = {
      contentResponseRef.set(IO.pure(content))
      val _ = when(downloader.streamBody(anyString(), anyString(), any[UUID]))
        .thenReturn(Stream.empty.covary[IO])
    }

    /**
     * Stub a download / extraction failure. The injected `extractText` reads from contentResponseRef, so flipping it to
     * `IO.raiseError` makes the resulting Stream fail when pulled. We also raise on `streamBody` so the failure is
     * visible regardless of which stage pulls first.
     */
    def stubDownloadFailure(error: Throwable): Unit = {
      contentResponseRef.set(IO.raiseError[String](error))
      val _ = when(downloader.streamBody(anyString(), anyString(), any[UUID]))
        .thenReturn(Stream.raiseError[IO](error))
    }

    /**
     * Stub the embedder to drain the chunk stream successfully and return Succeeded. Default behavior in
     * createFixture(); override per-test for failure scenarios. Uses `doAnswer().when()` form (NOT
     * `when().thenAnswer()`) because the latter invokes the method during stub-registration with null arguments, which
     * trips the existing default stub's stream-pulling logic.
     */
    def stubEmbedderSuccess(): Unit = {
      val _ = doAnswer { (invocation: org.mockito.invocation.InvocationOnMock) =>
        val ctx    = invocation.getArgument[BillEmbedCtx](0)
        val stream = invocation.getArgument[Stream[IO, String]](1)
        stream.compile.drain.as(ProcessingResult.Succeeded(ctx.naturalKey, eventEmitted = false))
      }.when(embedder).processChunks(any[BillEmbedCtx], any[Stream[IO, String]])
    }

    /** Stub the embedder to drain the chunk stream and return a Failed result (preserving errorClass). */
    def stubEmbedderFailed(reason: String, errorClass: String): Unit = {
      val _ = doAnswer { (invocation: org.mockito.invocation.InvocationOnMock) =>
        val ctx    = invocation.getArgument[BillEmbedCtx](0)
        val stream = invocation.getArgument[Stream[IO, String]](1)
        stream.compile.drain.as(ProcessingResult.Failed(ctx.naturalKey, reason, errorClass))
      }.when(embedder).processChunks(any[BillEmbedCtx], any[Stream[IO, String]])
    }

    /**
     * Stub the embedder to RAISE while pulling the chunk stream — i.e., the upstream error surfaces inside the
     * embedder.
     */
    def stubEmbedderPropagatesUpstreamError(): Unit = {
      val _ = doAnswer { (invocation: org.mockito.invocation.InvocationOnMock) =>
        val stream = invocation.getArgument[Stream[IO, String]](1)
        // Pull the stream so that any upstream raise (download / extract failure) surfaces here as IO.raiseError.
        stream.compile.drain.flatMap(_ => IO.raiseError[ProcessingResult](new IllegalStateException("unreachable")))
      }.when(embedder).processChunks(any[BillEmbedCtx], any[Stream[IO, String]])
    }

  }

  private def createFixture(): TestFixture = {
    val loggerMock = mock[PipelineLogger[IO]]
    when(loggerMock.info(any[LogContext], anyString())).thenReturn(IO.unit)
    when(loggerMock.warn(any[LogContext], anyString())).thenReturn(IO.unit)
    when(loggerMock.error(any[LogContext], anyString(), any[Option[Throwable]])).thenReturn(IO.unit)
    when(loggerMock.debug(any[LogContext], anyString())).thenReturn(IO.unit)

    // Repo mock — only deleteByVersionId is exercised by the processor now (insertMany lives inside the embedder).
    val rawRepoMock = mock[RawBillTextRepository[ConnectionIO]]
    when(rawRepoMock.deleteByVersionId(anyLong())).thenReturn(doobie.free.connection.unit)

    // Default: bill_text_versions has no row for the (billId, versionCode) pair, so the skip-check returns false and
    // processing proceeds. Tests that exercise the "already processed" skip path override this.
    val textVersionRepoMock = mock[BillTextVersionRepository[ConnectionIO]]
    when(textVersionRepoMock.findByBillId(any[Long]))
      .thenReturn(doobie.free.connection.pure(List.empty[BillTextVersionDO]))
    when(textVersionRepoMock.markFetched(anyLong(), any[Instant])).thenReturn(doobie.free.connection.unit)

    val embedderMock = mock[BillChunkEmbedder[IO]]
    // Default: embedder drains the stream and returns Succeeded. Per-test override via `stubEmbedderFailed` for
    // failure scenarios. Use `doAnswer().when()` form so subsequent re-stubs (which invoke the method to capture
    // its argument matchers) don't trigger this default thenAnswer with null args.
    val _ = doAnswer { (invocation: org.mockito.invocation.InvocationOnMock) =>
      val ctx    = invocation.getArgument[BillEmbedCtx](0)
      val stream = invocation.getArgument[Stream[IO, String]](1)
      stream.compile.drain.as(ProcessingResult.Succeeded(ctx.naturalKey, eventEmitted = false))
    }.when(embedderMock).processChunks(any[BillEmbedCtx], any[Stream[IO, String]])

    TestFixture(
      downloader = mock[BillTextDownloader[IO]],
      billRepository = mock[BillRepository[ConnectionIO]],
      textVersionRepository = textVersionRepoMock,
      rawBillTextRepository = rawRepoMock,
      embedder = embedderMock,
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
    when(f.textVersionRepository.storeAndUpdateBill(any[BillTextVersionDO])).thenReturn(doobie.free.connection.pure(1L))
    val _ = when(f.eventPublisher.billTextIngested(any[BillTextIngestedEvent], any[UUID]))
      .thenReturn(IO.pure("msg-id-123"))
  }

  // ===========================================================================
  // Happy path
  // ===========================================================================

  "processEvent" should "successfully process event end-to-end (delegate to embedder, mark fetched, publish)" in {
    val f     = createFixture()
    val event = makeEvent()
    stubSuccessfulFlow(f)

    val result = f.processor.processEvent(event, correlationId).unsafeRunSync()

    val _ = result.isSucceeded shouldBe true
    val _ = result.entityId shouldBe "118-HR-1"
    result shouldBe a[ProcessingResult.Succeeded]
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

  it should "call markFetched on the version repo after successful chunk persistence" in {
    val f     = createFixture()
    val event = makeEvent()
    stubSuccessfulFlow(f)

    val _ = f.processor.processEvent(event, correlationId).unsafeRunSync()

    verify(f.textVersionRepository, times(1)).markFetched(anyLong(), any[Instant])
  }

  it should "delegate the chunk stream to the embedder via processChunks" in {
    val f     = createFixture()
    val event = makeEvent()
    stubSuccessfulFlow(f)

    val _ = f.processor.processEvent(event, correlationId).unsafeRunSync()

    val ctxCaptor   = ArgumentCaptor.forClass(classOf[BillEmbedCtx])
    val _           = verify(f.embedder, times(1)).processChunks(ctxCaptor.capture(), any[Stream[IO, String]])
    val capturedCtx = ctxCaptor.getValue
    val _           = capturedCtx.naturalKey shouldBe "118-HR-1"
    capturedCtx.dbBillId shouldBe testDbBillId
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
    // The version row goes in with fetched_at = NULL; markFetched flips it to NOW() at the end of the streaming flow.
    stored.fetchedAt shouldBe None
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

  // ===========================================================================
  // Failure paths — bill not found, downloads, DB, publish
  // ===========================================================================

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

  it should "return Failed when DB store fails" in {
    val f     = createFixture()
    val event = makeEvent()
    stubBillLookup(f)
    f.stubSuccessfulDownload("some content")
    when(f.textVersionRepository.storeAndUpdateBill(any[BillTextVersionDO]))
      .thenReturn(doobie.free.connection.raiseError(BillTextProcessingFailed("118-HR-1", "DB connection lost")))

    val result = f.processor.processEvent(event, correlationId).unsafeRunSync()

    val _ = result.isFailed shouldBe true
    result.entityId shouldBe "118-HR-1"
  }

  it should "return Failed when event publish fails after successful store" in {
    val f     = createFixture()
    val event = makeEvent()
    stubBillLookup(f)
    f.stubSuccessfulDownload("some content")
    when(f.textVersionRepository.storeAndUpdateBill(any[BillTextVersionDO])).thenReturn(doobie.free.connection.pure(1L))
    when(f.eventPublisher.billTextIngested(any[BillTextIngestedEvent], any[UUID]))
      .thenReturn(IO.raiseError(BillTextProcessingFailed("118-HR-1", "Pub/Sub unavailable")))

    val result = f.processor.processEvent(event, correlationId).unsafeRunSync()

    val _ = result.isFailed shouldBe true
    result.entityId shouldBe "118-HR-1"
  }

  it should "not publish event when download fails" in {
    val f     = createFixture()
    val event = makeEvent()
    stubBillLookup(f)
    f.stubDownloadFailure(TextDownloadFailed(event.textUrl, event.textFormat, "HTTP 404"))
    when(f.textVersionRepository.storeAndUpdateBill(any[BillTextVersionDO])).thenReturn(doobie.free.connection.pure(1L))

    val _ = f.processor.processEvent(event, correlationId).unsafeRunSync()
    verify(f.eventPublisher, never()).billTextIngested(any[BillTextIngestedEvent], any[UUID])
  }

  it should "log error when processing fails" in {
    val f     = createFixture()
    val event = makeEvent()
    stubBillLookup(f)
    f.stubDownloadFailure(BillTextProcessingFailed("118-HR-1", "test failure"))
    when(f.textVersionRepository.storeAndUpdateBill(any[BillTextVersionDO])).thenReturn(doobie.free.connection.pure(1L))

    val _ = f.processor.processEvent(event, correlationId).unsafeRunSync()
    verify(f.logger, times(1)).error(any[LogContext], anyString(), any[Option[Throwable]])
  }

  // ===========================================================================
  // Embedder failure propagation — the new contract: don't reclassify the embedder's classification
  // ===========================================================================

  it should "propagate embedder Failed(Transient) without reclassifying" in {
    val f     = createFixture()
    val event = makeEvent()
    stubBillLookup(f)
    f.stubSuccessfulDownload("some content")
    f.stubEmbedderFailed("embedding model unavailable", "Transient")
    when(f.textVersionRepository.storeAndUpdateBill(any[BillTextVersionDO])).thenReturn(doobie.free.connection.pure(1L))

    val result = f.processor.processEvent(event, correlationId).unsafeRunSync()

    val _ = result.isFailed shouldBe true
    result match {
      case ProcessingResult.Failed(_, _, errorClass) => errorClass shouldBe "Transient"
      case other                                     => fail(s"Expected Failed but got $other")
    }
  }

  it should "propagate embedder Failed(Systemic) without reclassifying" in {
    val f     = createFixture()
    val event = makeEvent()
    stubBillLookup(f)
    f.stubSuccessfulDownload("some content")
    f.stubEmbedderFailed("input length exceeds context length", "Systemic")
    when(f.textVersionRepository.storeAndUpdateBill(any[BillTextVersionDO])).thenReturn(doobie.free.connection.pure(1L))

    val result = f.processor.processEvent(event, correlationId).unsafeRunSync()

    val _ = result.isFailed shouldBe true
    result match {
      case ProcessingResult.Failed(_, _, errorClass) => errorClass shouldBe "Systemic"
      case other                                     => fail(s"Expected Failed but got $other")
    }
  }

  it should "carry the embedder's failure reason into the propagated Failed result" in {
    val f     = createFixture()
    val event = makeEvent()
    stubBillLookup(f)
    f.stubSuccessfulDownload("some content")
    f.stubEmbedderFailed("ollama returned 503", "Transient")
    when(f.textVersionRepository.storeAndUpdateBill(any[BillTextVersionDO])).thenReturn(doobie.free.connection.pure(1L))

    val result = f.processor.processEvent(event, correlationId).unsafeRunSync()

    result match {
      case ProcessingResult.Failed(_, reason, _) => reason shouldBe "ollama returned 503"
      case other                                 => fail(s"Expected Failed but got $other")
    }
  }

  it should "NOT call markFetched when embedder reports Failed" in {
    val f     = createFixture()
    val event = makeEvent()
    stubBillLookup(f)
    f.stubSuccessfulDownload("some content")
    f.stubEmbedderFailed("embedding service down", "Transient")
    when(f.textVersionRepository.storeAndUpdateBill(any[BillTextVersionDO])).thenReturn(doobie.free.connection.pure(1L))

    val _ = f.processor.processEvent(event, correlationId).unsafeRunSync()

    verify(f.textVersionRepository, never()).markFetched(anyLong(), any[Instant])
  }

  it should "NOT publish event when embedder reports Failed" in {
    val f     = createFixture()
    val event = makeEvent()
    stubBillLookup(f)
    f.stubSuccessfulDownload("some content")
    f.stubEmbedderFailed("embedding service down", "Transient")
    when(f.textVersionRepository.storeAndUpdateBill(any[BillTextVersionDO])).thenReturn(doobie.free.connection.pure(1L))

    val _ = f.processor.processEvent(event, correlationId).unsafeRunSync()

    verify(f.eventPublisher, never()).billTextIngested(any[BillTextIngestedEvent], any[UUID])
  }

  // ===========================================================================
  // Error classification (for errors that surface BEFORE the embedder — i.e., download / extract / DB stage)
  // ===========================================================================

  it should "classify IO exceptions during download as Transient" in {
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

  it should "classify unknown exceptions as Systemic by default" in {
    val f     = createFixture()
    val event = makeEvent()
    stubBillLookup(f)
    f.stubDownloadFailure(BillTextProcessingFailed("118-HR-1", "unexpected error"))
    when(f.textVersionRepository.storeAndUpdateBill(any[BillTextVersionDO])).thenReturn(doobie.free.connection.pure(1L))

    val result = f.processor.processEvent(event, correlationId).unsafeRunSync()

    val _ = result.isFailed shouldBe true
    result match {
      case ProcessingResult.Failed(_, _, errorClass) => errorClass shouldBe "Systemic"
      case other                                     => fail(s"Expected Failed but got $other")
    }
  }

  // ===========================================================================
  // isAlreadyProcessed skip path
  // ===========================================================================

  "isAlreadyProcessed skip-check" should "return Skipped(\"already-processed\") when bill_text_versions row matches the event versionCode AND is fetched" in {
    val f     = createFixture()
    val event = makeEvent(naturalKey = "118-HR-1", versionCode = "ih")
    stubBillLookup(f)

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
    val _ = verify(f.embedder, never()).processChunks(any[BillEmbedCtx], any[Stream[IO, String]])
    val _ = verify(f.textVersionRepository, never()).storeAndUpdateBill(any[BillTextVersionDO])
    verify(f.eventPublisher, never()).billTextIngested(any[BillTextIngestedEvent], any[UUID])
  }

  it should "still call processFreshBillText when bill_text_versions has rows for OTHER versionCodes" in {
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
    val _ = verify(f.downloader, times(1)).streamBody(anyString(), anyString(), any[UUID])
    verify(f.eventPublisher, times(1)).billTextIngested(any[BillTextIngestedEvent], any[UUID])
  }

  it should "REPROCESS when the matching versionCode row exists but fetched_at is None (crashed mid-flight)" in {
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

  // ===========================================================================
  // stripNullBytes — pure helper, exercised here at the unit-method level
  // ===========================================================================

  "stripNullBytes" should "remove all NUL bytes (U+0000) from the input" in {
    val f         = createFixture()
    val processor = f.processor
    val withNuls  = "Bill" + 0.toChar.toString + "text" + 0.toChar.toString + "with" + 0.toChar.toString + "nulls"
    processor.stripNullBytes(withNuls) shouldBe "Billtextwithnulls"
  }

  it should "leave inputs without NUL bytes unchanged" in {
    val f         = createFixture()
    val processor = f.processor
    processor.stripNullBytes("Hello, world!") shouldBe "Hello, world!"
  }

  it should "return empty string for input that is entirely NUL bytes" in {
    val f         = createFixture()
    val processor = f.processor
    val allNuls   = 0.toChar.toString * 5
    processor.stripNullBytes(allNuls) shouldBe ""
  }

}
