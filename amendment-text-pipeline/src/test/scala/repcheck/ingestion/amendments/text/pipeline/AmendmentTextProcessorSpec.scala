package repcheck.ingestion.amendments.text.pipeline

import java.time.Instant
import java.util.UUID
import java.util.concurrent.atomic.AtomicReference

import cats.effect.IO
import cats.effect.unsafe.implicits.global

import fs2.Stream

import doobie._

import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.{any, anyInt, anyLong, anyString}
import org.mockito.Mockito.{never, times, verify, when}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar
import repcheck.ingestion.amendments.text.download.AmendmentTextDownloader
import repcheck.ingestion.amendments.text.embedding.{AmendmentChunkEmbedder, AmendmentEmbedCtx}
import repcheck.ingestion.amendments.text.errors.AmendmentTextProcessingFailed
import repcheck.ingestion.amendments.text.persistence.{AmendmentTextChunkRepository, AmendmentTextVersionRepository}
import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}
import repcheck.ingestion.text.embedding.EmbeddingConfig
import repcheck.pipeline.models.events.AmendmentTextAvailableEvent
import repcheck.pipeline.models.metadata.ProcessingResult
import repcheck.shared.models.congress.amendment.AmendmentType
import repcheck.shared.models.congress.dos.amendment.AmendmentTextVersionDO

/**
 * Unit specs for [[AmendmentTextProcessor]]. Mirrors `BillTextProcessorSpec` — drives the processor's narrow
 * responsibilities (upsert, skip-check via the upsert's `alreadyComplete` flag, orphan-chunk clear, hand stream to
 * embedder, mark fetched on success, surface embedder failures verbatim) with mocked dependencies.
 *
 * The cross-amendment embedder owns the per-chunk work and is exercised in its own spec; here we substitute a mock that
 * returns canned `ProcessingResult` values.
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
    downloader: AmendmentTextDownloader[IO],
    versionRepository: AmendmentTextVersionRepository[ConnectionIO],
    chunkRepository: AmendmentTextChunkRepository[ConnectionIO],
    embedder: AmendmentChunkEmbedder[IO],
    logger: PipelineLogger[IO],
    contentResponseRef: AtomicReference[IO[String]],
  ) {

    def processor: AmendmentTextProcessor[IO] =
      new AmendmentTextProcessor[IO](
        downloader = downloader,
        amendmentTextVersionRepository = versionRepository,
        amendmentTextChunkRepository = chunkRepository,
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

    def stubMarkFetchedOk(): Unit = {
      val _ = when(versionRepository.markFetched(anyLong(), any[Instant], anyInt()))
        .thenReturn(doobie.free.connection.unit)
    }

    def stubChunkRepoOk(): Unit = {
      val _ = when(chunkRepository.deleteByVersionId(anyLong())).thenReturn(doobie.free.connection.unit)
      val _ = when(chunkRepository.countByVersionId(anyLong())).thenReturn(doobie.free.connection.pure(3L))
      val _ = when(chunkRepository.sumContentLengthByVersionId(anyLong()))
        .thenReturn(doobie.free.connection.pure(36000L))
    }

    def stubEmbedder(result: ProcessingResult): Unit = {
      val _ = when(embedder.processChunks(any[AmendmentEmbedCtx], any[Stream[IO, String]]))
        .thenReturn(IO.pure(result))
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
      chunkRepository = mock[AmendmentTextChunkRepository[ConnectionIO]],
      embedder = mock[AmendmentChunkEmbedder[IO]],
      logger = noopLogger,
      contentResponseRef = new AtomicReference[IO[String]](IO.pure("")),
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

  "processEvent" should "short-circuit to Skipped when the upsert returns alreadyComplete = true" in {
    val f = newFixture
    f.stubUpsertReturning(testVersionId, inserted = false, alreadyComplete = true)
    f.stubSuccessfulDownload("body that should never be read")

    val result = f.processor.processEvent(buildEvent()).unsafeRunSync()
    val _      = result shouldBe ProcessingResult.Skipped(testNaturalKey, "already-ingested")
    val _      = verify(f.chunkRepository, never()).deleteByVersionId(anyLong())
    verify(f.embedder, never()).processChunks(any[AmendmentEmbedCtx], any[Stream[IO, String]])
  }

  it should "process a fresh version end-to-end on success" in {
    val f = newFixture
    f.stubUpsertReturning(testVersionId, inserted = true, alreadyComplete = false)
    f.stubChunkRepoOk()
    f.stubMarkFetchedOk()
    f.stubEmbedder(ProcessingResult.Succeeded(testNaturalKey, eventEmitted = false))
    f.stubSuccessfulDownload("amendment plain text")

    val result = f.processor.processEvent(buildEvent()).unsafeRunSync()
    val _      = result shouldBe ProcessingResult.Succeeded(testNaturalKey, eventEmitted = false)
    val _      = verify(f.chunkRepository, times(1)).deleteByVersionId(testVersionId)
    val _      = verify(f.versionRepository, times(1)).markFetched(any[Long], any[Instant], any[Int])
    verify(f.embedder, times(1)).processChunks(any[AmendmentEmbedCtx], any[Stream[IO, String]])
  }

  it should "process a re-submission update path (alreadyComplete = false even if not inserted)" in {
    val f = newFixture
    // Re-submission: existing row, not just-inserted, but version_date was newer so the WHERE clause refreshed
    // it (fetched_at reset to NULL). The repository surfaces alreadyComplete = false.
    f.stubUpsertReturning(testVersionId, inserted = false, alreadyComplete = false)
    f.stubChunkRepoOk()
    f.stubMarkFetchedOk()
    f.stubEmbedder(ProcessingResult.Succeeded(testNaturalKey, eventEmitted = false))
    f.stubSuccessfulDownload("re-submitted text")

    val result = f.processor.processEvent(buildEvent()).unsafeRunSync()
    val _      = result shouldBe ProcessingResult.Succeeded(testNaturalKey, eventEmitted = false)
    // Orphan chunks must be cleared before re-streaming.
    verify(f.chunkRepository, times(1)).deleteByVersionId(testVersionId)
  }

  it should "fail Systemic when the formatType is not HTML or PDF (no upsert, no embed)" in {
    val f      = newFixture
    val result = f.processor.processEvent(buildEvent(formatType = "XML")).unsafeRunSync()
    val _ = result match {
      case ProcessingResult.Failed(_, message, errorClass) =>
        val _ = errorClass shouldBe "Systemic"
        message should include("XML")
      case other => fail(s"Expected Failed(Systemic), got $other")
    }
    val _ = verify(f.versionRepository, never()).upsert(any[AmendmentTextVersionDO])
    verify(f.embedder, never()).processChunks(any[AmendmentEmbedCtx], any[Stream[IO, String]])
  }

  it should "surface the embedder's classified Failed verbatim without re-classifying" in {
    val f = newFixture
    f.stubUpsertReturning(testVersionId, inserted = true, alreadyComplete = false)
    f.stubChunkRepoOk()
    f.stubEmbedder(ProcessingResult.Failed(testNaturalKey, "embedder boom", "Transient"))
    f.stubSuccessfulDownload("text")

    val result = f.processor.processEvent(buildEvent()).unsafeRunSync()
    val _      = result shouldBe ProcessingResult.Failed(testNaturalKey, "embedder boom", "Transient")
    // Should NOT have called markFetched on a failed embedder result.
    verify(f.versionRepository, never()).markFetched(anyLong(), any[Instant], anyInt())
  }

  it should "fail Systemic when the embedder unexpectedly returns Skipped (contract violation)" in {
    val f = newFixture
    f.stubUpsertReturning(testVersionId, inserted = true, alreadyComplete = false)
    f.stubChunkRepoOk()
    f.stubEmbedder(ProcessingResult.Skipped(testNaturalKey, "no chunks"))
    f.stubSuccessfulDownload("text")

    val result = f.processor.processEvent(buildEvent()).unsafeRunSync()
    result match {
      case ProcessingResult.Failed(_, message, errorClass) =>
        val _ = errorClass shouldBe "Systemic"
        message should include("Skipped")
      case other => fail(s"Expected Failed(Systemic), got $other")
    }
  }

  it should "pass the correlationId through to the downloader so logs are threaded" in {
    val f = newFixture
    f.stubUpsertReturning(testVersionId, inserted = true, alreadyComplete = false)
    f.stubChunkRepoOk()
    f.stubMarkFetchedOk()
    f.stubEmbedder(ProcessingResult.Succeeded(testNaturalKey, eventEmitted = false))
    f.stubSuccessfulDownload("text")

    val _ = f.processor.processEvent(buildEvent()).unsafeRunSync()

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
    f.processor.stripNullBytes("a b c") shouldBe "abc"
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

}
