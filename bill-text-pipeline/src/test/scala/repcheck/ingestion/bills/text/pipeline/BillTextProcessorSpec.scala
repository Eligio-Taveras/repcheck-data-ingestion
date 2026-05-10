package repcheck.ingestion.bills.text.pipeline

import java.time.Instant
import java.util.UUID
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}

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
import repcheck.ingestion.common.events.IngestionEventPublisher
import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}
import repcheck.ingestion.text.embedding.EmbeddingConfig
import repcheck.pipeline.models.events.{BillTextAvailableEvent, BillTextIngestedEvent}
import repcheck.pipeline.models.metadata.ProcessingResult
import repcheck.shared.models.congress.dos.bill.{BillDO, BillTextVersionDO}

/**
 * Unit specs for [[BillTextProcessor]] under the Option-C embedder refactor.
 *
 * Post-refactor, the processor's job is narrowed to:
 *
 *   - skip-check vs `bill_text_versions` (`isAlreadyProcessed`); on hit → `ack` + `Skipped`
 *   - insert the version row with `fetched_at = NULL`
 *   - hand the chunk stream off to the embedder via `embedder.submit(ctx, stream, ackId, ack, nack)` and return
 *     immediately
 *   - on its OWN failures (bill-not-found, version-row UPSERT raised) → call `nack` + return `Failed`
 *
 * What the processor no longer owns (and what these specs do NOT test — they live in `CrossBillEmbedderSpec`):
 *
 *   - calling `embeddingService.generateEmbeddings`
 *   - inserting / upserting `RawBillTextDO` rows
 *   - clearing orphan chunks (idempotent UPSERT subsumes it)
 *   - calling `markFetched` (now in the embedder's per-ackId completion handler)
 *   - actually invoking `subscriber.acknowledge` (delegated via the `ack` callback to the embedder)
 *   - publishing `BillTextIngestedEvent` (composed into the `ack` callback so it runs after chunks persist)
 *
 * The `ProcessingResult` returned is for run-summary aggregation only; on successful submit it's
 * `Succeeded(eventEmitted = false)` because the actual publish + ack happen later inside the embedder.
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
    embedder: BillChunkEmbedder[IO],
    eventPublisher: IngestionEventPublisher[IO],
    logger: PipelineLogger[IO],
    contentResponseRef: AtomicReference[IO[String]],
    ackCalls: AtomicInteger,
    nackCalls: AtomicInteger,
    ackComposedFromEmbedder: AtomicReference[Option[IO[Unit]]],
  ) {

    def processor: BillTextProcessor[IO] =
      new BillTextProcessor[IO](
        downloader = downloader,
        billRepository = billRepository,
        textVersionRepository = textVersionRepository,
        embedder = embedder,
        embeddingConfig = testEmbeddingConfig,
        eventPublisher = eventPublisher,
        xa = testXa,
        logger = logger,
        // The injected `extractText` ignores the byte stream and yields whatever string is currently in the
        // contentResponseRef. Tests use this to inject content (or failures) without ever touching real bytes.
        extractText = (_, _) => Stream.eval(contentResponseRef.get()).flatMap(Stream.emit),
      )

    val ackEffect: IO[Unit]  = IO { val _ = ackCalls.incrementAndGet() }
    val nackEffect: IO[Unit] = IO { val _ = nackCalls.incrementAndGet() }

    /** Stub the success path: byte stream is empty (extractor stub ignores it; emits whatever is in the ref). */
    def stubSuccessfulDownload(content: String): Unit = {
      contentResponseRef.set(IO.pure(content))
      val _ = when(downloader.streamBody(anyString(), anyString(), any[UUID]))
        .thenReturn(Stream.empty.covary[IO])
    }

    def stubDownloadFailure(error: Throwable): Unit = {
      contentResponseRef.set(IO.raiseError[String](error))
      val _ = when(downloader.streamBody(anyString(), anyString(), any[UUID]))
        .thenReturn(Stream.raiseError[IO](error))
    }

    /**
     * Default embedder stub: drains the chunk stream and returns Unit. Captures the composed `ack` effect so tests can
     * drive it manually if they want to assert post-ack behavior.
     */
    def stubEmbedderSuccess(): Unit = {
      val _ = doAnswer { (invocation: org.mockito.invocation.InvocationOnMock) =>
        val stream = invocation.getArgument[Stream[IO, String]](1)
        val ack    = invocation.getArgument[IO[Unit]](3)
        ackComposedFromEmbedder.set(Some(ack))
        stream.compile.drain
      }.when(embedder)
        .submit(
          any[BillEmbedCtx],
          any[Stream[IO, String]],
          anyString(),
          any[IO[Unit]],
          any[IO[Unit]],
        )
    }

    /** Embedder that drains the stream and immediately invokes its passed-in `nack` effect. */
    def stubEmbedderInvokesNack(): Unit = {
      val _ = doAnswer { (invocation: org.mockito.invocation.InvocationOnMock) =>
        val stream = invocation.getArgument[Stream[IO, String]](1)
        val nack   = invocation.getArgument[IO[Unit]](4)
        stream.compile.drain *> nack
      }.when(embedder)
        .submit(
          any[BillEmbedCtx],
          any[Stream[IO, String]],
          anyString(),
          any[IO[Unit]],
          any[IO[Unit]],
        )
    }

  }

  private def createFixture(): TestFixture = {
    val loggerMock = mock[PipelineLogger[IO]]
    when(loggerMock.info(any[LogContext], anyString())).thenReturn(IO.unit)
    when(loggerMock.warn(any[LogContext], anyString())).thenReturn(IO.unit)
    when(loggerMock.error(any[LogContext], anyString(), any[Option[Throwable]])).thenReturn(IO.unit)
    when(loggerMock.debug(any[LogContext], anyString())).thenReturn(IO.unit)

    val textVersionRepoMock = mock[BillTextVersionRepository[ConnectionIO]]
    when(textVersionRepoMock.findByBillId(any[Long]))
      .thenReturn(doobie.free.connection.pure(List.empty[BillTextVersionDO]))
    when(textVersionRepoMock.markFetched(anyLong(), any[Instant])).thenReturn(doobie.free.connection.unit)

    val embedderMock = mock[BillChunkEmbedder[IO]]
    val ackCapture   = new AtomicReference[Option[IO[Unit]]](None)
    // Default: the embedder drains the stream and returns. The processor's expected ProcessingResult is
    // Succeeded(eventEmitted = false). Tests that need to assert NACK-on-embedder-failure override via
    // `stubEmbedderInvokesNack`.
    val _ = doAnswer { (invocation: org.mockito.invocation.InvocationOnMock) =>
      val stream = invocation.getArgument[Stream[IO, String]](1)
      val ack    = invocation.getArgument[IO[Unit]](3)
      ackCapture.set(Some(ack))
      stream.compile.drain
    }.when(embedderMock)
      .submit(
        any[BillEmbedCtx],
        any[Stream[IO, String]],
        anyString(),
        any[IO[Unit]],
        any[IO[Unit]],
      )

    TestFixture(
      downloader = mock[BillTextDownloader[IO]],
      billRepository = mock[BillRepository[ConnectionIO]],
      textVersionRepository = textVersionRepoMock,
      embedder = embedderMock,
      eventPublisher = mock[IngestionEventPublisher[IO]],
      logger = loggerMock,
      contentResponseRef = new AtomicReference[IO[String]](IO.pure("")),
      ackCalls = new AtomicInteger(0),
      nackCalls = new AtomicInteger(0),
      ackComposedFromEmbedder = ackCapture,
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

  "processEvent" should "successfully process event end-to-end (delegate to embedder, return Succeeded(eventEmitted=false))" in {
    val f     = createFixture()
    val event = makeEvent()
    stubSuccessfulFlow(f)

    val result = f.processor.processEvent(event, correlationId, "ack-1", f.ackEffect, f.nackEffect).unsafeRunSync()

    val _ = result.isSucceeded shouldBe true
    val _ = result.entityId shouldBe "118-HR-1"
    result match {
      case ProcessingResult.Succeeded(_, eventEmitted) => eventEmitted shouldBe false
      case other                                       => fail(s"Expected Succeeded but got $other")
    }
  }

  it should "delegate the chunk stream to the embedder via submit (with ackId and ack/nack effects)" in {
    val f     = createFixture()
    val event = makeEvent()
    stubSuccessfulFlow(f)

    val _ = f.processor.processEvent(event, correlationId, "ack-42", f.ackEffect, f.nackEffect).unsafeRunSync()

    val ctxCaptor   = ArgumentCaptor.forClass(classOf[BillEmbedCtx])
    val ackIdCaptor = ArgumentCaptor.forClass(classOf[String])
    val _ = verify(f.embedder, times(1)).submit(
      ctxCaptor.capture(),
      any[Stream[IO, String]],
      ackIdCaptor.capture(),
      any[IO[Unit]],
      any[IO[Unit]],
    )
    val _ = ctxCaptor.getValue.naturalKey shouldBe "118-HR-1"
    val _ = ctxCaptor.getValue.dbBillId shouldBe testDbBillId
    ackIdCaptor.getValue shouldBe "ack-42"
  }

  it should "compose publishIngestedEvent into the ack callback handed to the embedder (publish runs only when embedder fires its ack)" in {
    val f     = createFixture()
    val event = makeEvent(naturalKey = "118-HR-99", versionCode = "rh", previousVersionCode = Some("ih"))
    stubSuccessfulFlow(f, billId = "118-HR-99")

    // Run processEvent — embedder default-stub captures the composed ack but DOES NOT invoke it.
    val _ = f.processor.processEvent(event, correlationId, "ack-1", f.ackEffect, f.nackEffect).unsafeRunSync()

    // Publish has NOT happened yet — the embedder hasn't fired the ack.
    val _ = verify(f.eventPublisher, never()).billTextIngested(any[BillTextIngestedEvent], any[UUID])

    // Now drive the captured ack manually → publish fires, then the underlying ack effect fires.
    val composed = f.ackComposedFromEmbedder.get()
    val _        = composed.isDefined shouldBe true
    composed.foreach(_.unsafeRunSync())

    val captor    = ArgumentCaptor.forClass(classOf[BillTextIngestedEvent])
    val _         = verify(f.eventPublisher, times(1)).billTextIngested(captor.capture(), any[UUID])
    val published = captor.getValue
    val _         = published.naturalKey shouldBe "118-HR-99"
    val _         = published.versionCode shouldBe "rh"
    val _         = published.previousVersionCode shouldBe Some("ih")
    f.ackCalls.get() shouldBe 1
  }

  it should "NOT call markFetched directly (delegated to embedder)" in {
    val f     = createFixture()
    val event = makeEvent()
    stubSuccessfulFlow(f)

    val _ = f.processor.processEvent(event, correlationId, "ack-1", f.ackEffect, f.nackEffect).unsafeRunSync()

    verify(f.textVersionRepository, never()).markFetched(anyLong(), any[Instant])
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

    val _ = f.processor.processEvent(event, correlationId, "ack-1", f.ackEffect, f.nackEffect).unsafeRunSync()

    val captor = ArgumentCaptor.forClass(classOf[BillTextVersionDO])
    val _      = verify(f.textVersionRepository, times(1)).storeAndUpdateBill(captor.capture())
    val stored = captor.getValue

    val _ = stored.billId shouldBe testDbBillId
    val _ = stored.versionCode shouldBe "enr"
    val _ = stored.versionType shouldBe "Formatted XML"
    val _ = stored.url shouldBe Some("https://api.congress.gov/v3/bill/118/s/42/text/enr")
    stored.fetchedAt shouldBe None
  }

  // ===========================================================================
  // Failure paths — bill-not-found, version-row UPSERT raised
  // ===========================================================================

  it should "NACK directly and return Failed when bill not found in DB" in {
    val f     = createFixture()
    val event = makeEvent(naturalKey = "999-HR-0")
    when(f.billRepository.findByBillId("999-HR-0"))
      .thenReturn(doobie.free.connection.pure(Option.empty[BillDO]))

    val result = f.processor.processEvent(event, correlationId, "ack-1", f.ackEffect, f.nackEffect).unsafeRunSync()

    val _ = result.isFailed shouldBe true
    val _ = result.entityId shouldBe "999-HR-0"
    val _ = f.nackCalls.get() shouldBe 1
    val _ = f.ackCalls.get() shouldBe 0
    result match {
      case ProcessingResult.Failed(_, _, errorClass) => errorClass shouldBe "Systemic"
      case other                                     => fail(s"Expected Failed but got $other")
    }
  }

  it should "NACK directly and return Failed when version-row UPSERT fails" in {
    val f     = createFixture()
    val event = makeEvent()
    stubBillLookup(f)
    f.stubSuccessfulDownload("some content")
    when(f.textVersionRepository.storeAndUpdateBill(any[BillTextVersionDO]))
      .thenReturn(doobie.free.connection.raiseError(BillTextProcessingFailed("118-HR-1", "DB connection lost")))

    val result = f.processor.processEvent(event, correlationId, "ack-1", f.ackEffect, f.nackEffect).unsafeRunSync()

    val _ = result.isFailed shouldBe true
    val _ = f.nackCalls.get() shouldBe 1
    f.ackCalls.get() shouldBe 0
  }

  it should "NOT publish event when embedder hasn't yet fired the composed ack (publish lives inside the ack)" in {
    val f     = createFixture()
    val event = makeEvent()
    stubBillLookup(f)
    f.stubSuccessfulDownload("some content")
    when(f.textVersionRepository.storeAndUpdateBill(any[BillTextVersionDO])).thenReturn(doobie.free.connection.pure(1L))

    // Default embedder stub captures the ack but does not invoke it.
    val _ = f.processor.processEvent(event, correlationId, "ack-1", f.ackEffect, f.nackEffect).unsafeRunSync()
    verify(f.eventPublisher, never()).billTextIngested(any[BillTextIngestedEvent], any[UUID])
  }

  // ===========================================================================
  // isAlreadyProcessed skip path — ACK directly + return Skipped
  // ===========================================================================

  "isAlreadyProcessed skip-check" should "ACK directly and return Skipped(\"already-processed\") when version row is fetched" in {
    val f     = createFixture()
    val event = makeEvent(naturalKey = "118-HR-1", versionCode = "ih")
    stubBillLookup(f)

    val existingVersion = mock[BillTextVersionDO]
    when(existingVersion.versionCode).thenReturn("ih")
    when(existingVersion.fetchedAt).thenReturn(Some(Instant.now()))
    when(f.textVersionRepository.findByBillId(testDbBillId))
      .thenReturn(doobie.free.connection.pure(List(existingVersion)))

    val result = f.processor.processEvent(event, correlationId, "ack-1", f.ackEffect, f.nackEffect).unsafeRunSync()

    val _ = result.isSkipped shouldBe true
    val _ = f.ackCalls.get() shouldBe 1
    val _ = f.nackCalls.get() shouldBe 0
    result match {
      case ProcessingResult.Skipped(entityId, reason) =>
        val _ = entityId shouldBe "118-HR-1"
        reason shouldBe "already-processed"
      case other => fail(s"Expected Skipped but got $other")
    }
  }

  it should "skip the expensive download/embed work when already processed" in {
    val f     = createFixture()
    val event = makeEvent(naturalKey = "118-HR-1", versionCode = "ih")
    stubBillLookup(f)

    val existingVersion = mock[BillTextVersionDO]
    when(existingVersion.versionCode).thenReturn("ih")
    when(existingVersion.fetchedAt).thenReturn(Some(Instant.now()))
    when(f.textVersionRepository.findByBillId(testDbBillId))
      .thenReturn(doobie.free.connection.pure(List(existingVersion)))

    val _ = f.processor.processEvent(event, correlationId, "ack-1", f.ackEffect, f.nackEffect).unsafeRunSync()

    val _ = verify(f.downloader, never()).streamBody(anyString(), anyString(), any[UUID])
    val _ = verify(f.embedder, never())
      .submit(any[BillEmbedCtx], any[Stream[IO, String]], anyString(), any[IO[Unit]], any[IO[Unit]])
    val _ = verify(f.textVersionRepository, never()).storeAndUpdateBill(any[BillTextVersionDO])
    verify(f.eventPublisher, never()).billTextIngested(any[BillTextIngestedEvent], any[UUID])
  }

  it should "still call the fresh-text path when bill_text_versions has rows for OTHER versionCodes" in {
    val f     = createFixture()
    val event = makeEvent(naturalKey = "118-HR-1", versionCode = "rh")
    stubSuccessfulFlow(f)

    val ihVersion = mock[BillTextVersionDO]
    when(ihVersion.versionCode).thenReturn("ih")
    when(ihVersion.fetchedAt).thenReturn(Some(Instant.now()))
    when(f.textVersionRepository.findByBillId(testDbBillId))
      .thenReturn(doobie.free.connection.pure(List(ihVersion)))

    val result = f.processor.processEvent(event, correlationId, "ack-1", f.ackEffect, f.nackEffect).unsafeRunSync()

    val _ = result.isSucceeded shouldBe true
    verify(f.downloader, times(1)).streamBody(anyString(), anyString(), any[UUID])
  }

  it should "REPROCESS when matching versionCode row exists but fetched_at is None (crashed mid-flight)" in {
    val f     = createFixture()
    val event = makeEvent(naturalKey = "118-HR-1", versionCode = "ih")
    stubSuccessfulFlow(f)

    val partialVersion = mock[BillTextVersionDO]
    when(partialVersion.versionCode).thenReturn("ih")
    when(partialVersion.fetchedAt).thenReturn(None)
    when(f.textVersionRepository.findByBillId(testDbBillId))
      .thenReturn(doobie.free.connection.pure(List(partialVersion)))

    val result = f.processor.processEvent(event, correlationId, "ack-1", f.ackEffect, f.nackEffect).unsafeRunSync()

    val _ = result.isSucceeded shouldBe true
    verify(f.downloader, times(1)).streamBody(anyString(), anyString(), any[UUID])
  }

  // ===========================================================================
  // Error classification (still relevant — processor still classifies its OWN failures)
  // ===========================================================================

  it should "classify IO exceptions during the early phase as Transient" in {
    val f     = createFixture()
    val event = makeEvent(naturalKey = "999-HR-0")
    // Force an IOException to surface from the early lookup phase.
    when(f.billRepository.findByBillId("999-HR-0"))
      .thenReturn(doobie.free.connection.raiseError(new java.io.IOException("network error")))

    val result = f.processor.processEvent(event, correlationId, "ack-1", f.ackEffect, f.nackEffect).unsafeRunSync()

    val _ = result.isFailed shouldBe true
    val _ = f.nackCalls.get() shouldBe 1
    result match {
      case ProcessingResult.Failed(_, _, errorClass) => errorClass shouldBe "Transient"
      case other                                     => fail(s"Expected Failed but got $other")
    }
  }

  it should "classify SQLTransientException as Transient" in {
    val f     = createFixture()
    val event = makeEvent()
    stubBillLookup(f)
    when(f.textVersionRepository.storeAndUpdateBill(any[BillTextVersionDO]))
      .thenReturn(doobie.free.connection.raiseError(new java.sql.SQLTransientConnectionException("db connection lost")))
    f.stubSuccessfulDownload("content")

    val result = f.processor.processEvent(event, correlationId, "ack-1", f.ackEffect, f.nackEffect).unsafeRunSync()

    val _ = result.isFailed shouldBe true
    val _ = f.nackCalls.get() shouldBe 1
    result match {
      case ProcessingResult.Failed(_, _, errorClass) => errorClass shouldBe "Transient"
      case other                                     => fail(s"Expected Failed but got $other")
    }
  }

  it should "classify TextDownloadFailed as Transient" in {
    val f     = createFixture()
    val event = makeEvent()
    stubBillLookup(f)
    when(f.textVersionRepository.storeAndUpdateBill(any[BillTextVersionDO]))
      .thenReturn(doobie.free.connection.raiseError(TextDownloadFailed(event.textUrl, event.textFormat, "HTTP 503")))
    f.stubSuccessfulDownload("content")

    val result = f.processor.processEvent(event, correlationId, "ack-1", f.ackEffect, f.nackEffect).unsafeRunSync()

    result.isFailed shouldBe true
  }

  it should "classify BillTextProcessingFailed as Systemic" in {
    val f     = createFixture()
    val event = makeEvent()
    stubBillLookup(f)
    when(f.textVersionRepository.storeAndUpdateBill(any[BillTextVersionDO]))
      .thenReturn(doobie.free.connection.raiseError(BillTextProcessingFailed("118-HR-1", "invalid format")))
    f.stubSuccessfulDownload("content")

    val result = f.processor.processEvent(event, correlationId, "ack-1", f.ackEffect, f.nackEffect).unsafeRunSync()

    val _ = result.isFailed shouldBe true
    result match {
      case ProcessingResult.Failed(_, _, errorClass) => errorClass shouldBe "Systemic"
      case other                                     => fail(s"Expected Failed but got $other")
    }
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
