package repcheck.ingestion.amendments.text.pipeline

import java.time.Instant
import java.util.UUID
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}

import cats.effect.IO
import cats.effect.unsafe.implicits.global

import fs2.Stream

import doobie._

import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.{any, anyLong, anyString, eq => eqTo}
import org.mockito.Mockito.{never, times, verify, when}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar
import repcheck.ingestion.amendments.text.download.AmendmentTextDownloader
import repcheck.ingestion.amendments.text.embedding.{AmendmentChunkEmbedder, AmendmentEmbedCtx}
import repcheck.ingestion.amendments.text.errors.{AmendmentTextDownloadHttpError, AmendmentTextProcessingFailed}
import repcheck.ingestion.amendments.text.persistence.AmendmentTextVersionRepository
import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}
import repcheck.ingestion.text.embedding.EmbeddingConfig
import repcheck.pipeline.models.events.AmendmentTextAvailableEvent
import repcheck.pipeline.models.metadata.ProcessingResult
import repcheck.shared.models.congress.amendment.AmendmentType
import repcheck.shared.models.congress.dos.amendment.AmendmentTextVersionDO

/**
 * Unit specs for [[AmendmentTextProcessor]].
 *
 * The processor's narrow responsibilities under the queue+ack-delegation refactor:
 *   - resolve format / build version DO / call `upsert`
 *   - on `alreadyComplete = true`: fire the supplied `ack` synchronously, return `Skipped`
 *   - otherwise: hand the chunk stream off to `embedder.submit` (which owns trim + markFetched + the eventual ACK) and
 *     return `Succeeded` as a stats signal
 *   - on any exception: fire the supplied `nack`, return `Failed`
 *
 * The cross-amendment embedder is mocked here; its own behavior is exercised in `CrossAmendmentEmbedderSpec`. The
 * processor no longer touches the chunk repository at all (chunk persistence + trim moved to the embedder).
 */
class AmendmentTextProcessorSpec extends AnyFlatSpec with Matchers with MockitoSugar {

  private val testXa: Transactor[IO] = Transactor.fromDriverManager[IO](
    driver = "org.h2.Driver",
    url = "jdbc:h2:mem:test-amend-text-processor;DB_CLOSE_DELAY=-1",
    user = "",
    password = "",
    logHandler = None,
  )

  private val correlationId       = UUID.randomUUID()
  private val testAmendmentId     = 42L
  private val testNaturalKey      = "117-SAMDT-2137"
  private val testVersionId: Long = 7L
  private val testAckId: String   = "ack-test-1"

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

  final private class AckNackProbe {
    val acks           = new AtomicInteger(0)
    val nacks          = new AtomicInteger(0)
    val ack: IO[Unit]  = IO { val _ = acks.incrementAndGet(); () }
    val nack: IO[Unit] = IO { val _ = nacks.incrementAndGet(); () }
  }

  private case class TestFixture(
    downloader: AmendmentTextDownloader[IO],
    versionRepository: AmendmentTextVersionRepository[ConnectionIO],
    embedder: AmendmentChunkEmbedder[IO],
    logger: PipelineLogger[IO],
    contentResponseRef: AtomicReference[IO[String]],
    probe: AckNackProbe,
  ) {

    def processor: AmendmentTextProcessor[IO] =
      new AmendmentTextProcessor[IO](
        downloader = downloader,
        amendmentTextVersionRepository = versionRepository,
        embedder = embedder,
        embeddingConfig = testEmbeddingConfig,
        xa = testXa,
        logger = logger,
        extractText = (_, _, _) => Stream.eval(contentResponseRef.get()).flatMap(Stream.emit),
      )

    def stubSuccessfulDownload(content: String): Unit = {
      contentResponseRef.set(IO.pure(content))
      val _ = when(downloader.streamBody(anyString(), anyString(), any[UUID]))
        .thenReturn(Stream.empty.covary[IO])
    }

    def stubUpsertReturning(versionId: Long, inserted: Boolean, alreadyComplete: Boolean): Unit = {
      val _ = when(versionRepository.upsert(any[AmendmentTextVersionDO]))
        .thenReturn(doobie.free.connection.pure((versionId, inserted, alreadyComplete)))
    }

    def stubEmbedderOk(): Unit = {
      val _ = when(
        embedder.submit(
          any[AmendmentEmbedCtx],
          any[Stream[IO, String]],
          anyString(),
          any[IO[Unit]],
          any[IO[Unit]],
        )
      ).thenReturn(IO.unit)
    }

    def stubEmbedderRaises(error: Throwable): Unit = {
      val _ = when(
        embedder.submit(
          any[AmendmentEmbedCtx],
          any[Stream[IO, String]],
          anyString(),
          any[IO[Unit]],
          any[IO[Unit]],
        )
      ).thenReturn(IO.raiseError[Unit](error))
    }

  }

  private def newFixture: TestFixture = {
    val noopLogger = new PipelineLogger[IO] {
      def info(context: LogContext, message: String): IO[Unit]                            = IO.unit
      def warn(context: LogContext, message: String): IO[Unit]                            = IO.unit
      def error(context: LogContext, message: String, cause: Option[Throwable]): IO[Unit] = IO.unit
      def debug(context: LogContext, message: String): IO[Unit]                           = IO.unit
    }
    TestFixture(
      downloader = mock[AmendmentTextDownloader[IO]],
      versionRepository = mock[AmendmentTextVersionRepository[ConnectionIO]],
      embedder = mock[AmendmentChunkEmbedder[IO]],
      logger = noopLogger,
      contentResponseRef = new AtomicReference[IO[String]](IO.pure("")),
      probe = new AckNackProbe,
    )
  }

  private def buildEvent(versionTypeCode: String = "SUB", formatType: String = "HTML"): AmendmentTextAvailableEvent =
    AmendmentTextAvailableEvent(
      amendmentId = testAmendmentId,
      naturalKey = testNaturalKey,
      congress = 117,
      amendmentType = AmendmentType.SAMDT,
      number = "2137",
      versionTypeCode = versionTypeCode,
      formatType = formatType,
      url = "https://www.congress.gov/117/crec/2021/08/01/167/136/CREC-2021-08-01-pt1-PgS5255.htm",
      publishedDate = Some(Instant.parse("2021-08-01T04:00:00Z")),
      correlationId = correlationId,
    )

  private def runProcess(f: TestFixture, event: AmendmentTextAvailableEvent = buildEvent()): ProcessingResult =
    f.processor.processEvent(event, testAckId, f.probe.ack, f.probe.nack).unsafeRunSync()

  "processEvent" should "fire ack synchronously and return Skipped when upsert reports alreadyComplete = true" in {
    val f = newFixture
    f.stubUpsertReturning(testVersionId, inserted = false, alreadyComplete = true)
    f.stubSuccessfulDownload("body that should never be read")

    val result = runProcess(f)
    val _      = result shouldBe ProcessingResult.Skipped(testNaturalKey, "already-ingested")
    val _      = f.probe.acks.get() shouldBe 1
    val _      = f.probe.nacks.get() shouldBe 0
    verify(f.embedder, never())
      .submit(any[AmendmentEmbedCtx], any[Stream[IO, String]], anyString(), any[IO[Unit]], any[IO[Unit]])
  }

  it should "delegate ACK to the embedder for a fresh version (processor returns Succeeded; ack fired by embedder later)" in {
    val f = newFixture
    f.stubUpsertReturning(testVersionId, inserted = true, alreadyComplete = false)
    f.stubEmbedderOk()
    f.stubSuccessfulDownload("amendment plain text")

    val result = runProcess(f)
    val _      = result shouldBe ProcessingResult.Succeeded(testNaturalKey, eventEmitted = false)
    // Processor itself does NOT ack — the embedder owns ACK after chunk persistence.
    val _ = f.probe.acks.get() shouldBe 0
    val _ = f.probe.nacks.get() shouldBe 0
    val _ =
      verify(f.embedder, times(1))
        .submit(any[AmendmentEmbedCtx], any[Stream[IO, String]], eqTo(testAckId), any[IO[Unit]], any[IO[Unit]])
    // Processor must NOT call markFetched — that's the embedder's job.
    verify(f.versionRepository, never()).markFetched(anyLong(), any[Instant], any[Int])
  }

  it should "process a re-submission (alreadyComplete = false even if not inserted)" in {
    val f = newFixture
    f.stubUpsertReturning(testVersionId, inserted = false, alreadyComplete = false)
    f.stubEmbedderOk()
    f.stubSuccessfulDownload("re-submitted text")

    val result = runProcess(f)
    val _      = result shouldBe ProcessingResult.Succeeded(testNaturalKey, eventEmitted = false)
    // Embedder receives the chunk stream; chunk-layer idempotency (LWW UPSERT + trim) handles re-submission.
    verify(f.embedder, times(1))
      .submit(any[AmendmentEmbedCtx], any[Stream[IO, String]], anyString(), any[IO[Unit]], any[IO[Unit]])
  }

  it should "fail Systemic and NACK when the formatType is not HTML or PDF (no upsert, no embed)" in {
    val f      = newFixture
    val result = runProcess(f, buildEvent(formatType = "XML"))
    val _ = result match {
      case ProcessingResult.Failed(_, message, errorClass) =>
        val _ = errorClass shouldBe "Systemic"
        message should include("XML")
      case other => fail(s"Expected Failed(Systemic), got $other")
    }
    val _ = f.probe.nacks.get() shouldBe 1
    val _ = f.probe.acks.get() shouldBe 0
    val _ = verify(f.versionRepository, never()).upsert(any[AmendmentTextVersionDO])
    verify(f.embedder, never())
      .submit(any[AmendmentEmbedCtx], any[Stream[IO, String]], anyString(), any[IO[Unit]], any[IO[Unit]])
  }

  it should "NACK and report Failed when the version-row upsert raises" in {
    val f = newFixture
    val _ = when(f.versionRepository.upsert(any[AmendmentTextVersionDO]))
      .thenReturn(doobie.free.connection.raiseError(new java.sql.SQLException("upsert exploded")))
    f.stubSuccessfulDownload("text")

    val result = runProcess(f)
    val _ = result match {
      case _: ProcessingResult.Failed => succeed
      case other                      => fail(s"Expected Failed, got $other")
    }
    val _ = f.probe.nacks.get() shouldBe 1
    val _ = f.probe.acks.get() shouldBe 0
    verify(f.embedder, never())
      .submit(any[AmendmentEmbedCtx], any[Stream[IO, String]], anyString(), any[IO[Unit]], any[IO[Unit]])
  }

  it should "NACK and report Failed when embedder.submit raises (embedder already NACKed internally — processor's NACK is harmless duplicate or failsafe)" in {
    val f = newFixture
    f.stubUpsertReturning(testVersionId, inserted = true, alreadyComplete = false)
    f.stubEmbedderRaises(new java.io.IOException("submit failed"))
    f.stubSuccessfulDownload("text")

    val result = runProcess(f)
    val _ = result match {
      case ProcessingResult.Failed(_, _, errorClass) => errorClass shouldBe "Transient"
      case other                                     => fail(s"Expected Failed(Transient), got $other")
    }
    // Processor invokes its own NACK on the failure path. The embedder's internal NACK before re-raise is also live;
    // the contract is "at-least-once NACK" — Pub/Sub treats a duplicate modifyAckDeadline(0) as idempotent.
    val _ = f.probe.nacks.get() shouldBe 1
    f.probe.acks.get() shouldBe 0
  }

  it should "pass the correlationId through to the downloader so logs are threaded" in {
    val f = newFixture
    f.stubUpsertReturning(testVersionId, inserted = true, alreadyComplete = false)
    f.stubEmbedderOk()
    f.stubSuccessfulDownload("text")

    val _ = runProcess(f)

    val captor = ArgumentCaptor.forClass(classOf[UUID])
    val _      = verify(f.downloader).streamBody(anyString(), anyString(), captor.capture())
    captor.getValue shouldBe correlationId
  }

  "buildTextVersion" should "preserve the wire versionTypeCode verbatim (no translation)" in {
    val f     = newFixture
    val event = buildEvent(versionTypeCode = "SUB", formatType = "HTML")
    val version =
      f.processor.buildTextVersion(event, formatType = repcheck.shared.models.congress.common.FormatType.FormattedText)
    val _ = version.formatType.text shouldBe "Formatted Text"
    val _ = version.amendmentId shouldBe testAmendmentId
    val _ = version.versionType shouldBe "SUB"
    version.url shouldBe event.url
  }

  it should "carry through MOD when the wire code is MOD" in {
    val f     = newFixture
    val event = buildEvent(versionTypeCode = "MOD", formatType = "PDF")
    val version =
      f.processor.buildTextVersion(event, formatType = repcheck.shared.models.congress.common.FormatType.PDF)
    val _ = version.versionType shouldBe "MOD"
    version.formatType.text shouldBe "PDF"
  }

  it should "fall back to Instant.EPOCH when publishedDate is None" in {
    val f     = newFixture
    val event = buildEvent().copy(publishedDate = None)
    val v =
      f.processor.buildTextVersion(event, formatType = repcheck.shared.models.congress.common.FormatType.FormattedText)
    v.versionDate shouldBe Instant.EPOCH
  }

  "stripNullBytes" should "remove U+0000 from a string" in {
    val f = newFixture
    f.processor.stripNullBytes("abc") shouldBe "abc"
  }

  it should "leave a normal string untouched" in {
    val f = newFixture
    f.processor.stripNullBytes("normal text") shouldBe "normal text"
  }

  "classifyError" should "classify all known cases per the bill-side pattern" in {
    val f = newFixture
    val _ = f.processor.classifyError(AmendmentTextProcessingFailed("X", "Y")) shouldBe "Systemic"
    val _ = f.processor.classifyError(new java.io.IOException("net glitch")) shouldBe "Transient"
    val _ = f.processor.classifyError(new java.net.SocketTimeoutException("t")) shouldBe "Transient"
    val _ = f.processor.classifyError(new java.net.ConnectException("refused")) shouldBe "Transient"
    val _ = f.processor.classifyError(new java.sql.SQLTransientException("t")) shouldBe "Transient"
    f.processor.classifyError(new RuntimeException("unknown")) shouldBe "Systemic"
  }

  it should "route AmendmentTextDownloadHttpError through the typed classifier (429/5xx Transient, 4xx Systemic)" in {
    val f = newFixture
    val _ = f.processor.classifyError(AmendmentTextDownloadHttpError(429, "rate limit")) shouldBe "Transient"
    val _ = f.processor.classifyError(AmendmentTextDownloadHttpError(500, "")) shouldBe "Transient"
    val _ = f.processor.classifyError(AmendmentTextDownloadHttpError(502, "")) shouldBe "Transient"
    val _ = f.processor.classifyError(AmendmentTextDownloadHttpError(503, "")) shouldBe "Transient"
    val _ = f.processor.classifyError(AmendmentTextDownloadHttpError(504, "")) shouldBe "Transient"
    val _ = f.processor.classifyError(AmendmentTextDownloadHttpError(401, "Unauthorized")) shouldBe "Systemic"
    val _ = f.processor.classifyError(AmendmentTextDownloadHttpError(403, "Forbidden")) shouldBe "Systemic"
    f.processor.classifyError(AmendmentTextDownloadHttpError(400, "Bad Request")) shouldBe "Systemic"
  }

}
