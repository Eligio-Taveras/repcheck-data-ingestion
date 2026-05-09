package repcheck.ingestion.amendments.text.embedding

import java.util.concurrent.atomic.AtomicReference

import scala.concurrent.duration._

import cats.effect.IO
import cats.effect.unsafe.implicits.global

import fs2.Stream

import doobie._

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import repcheck.ingestion.amendments.text.persistence.AmendmentTextChunkRepository
import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}
import repcheck.ingestion.text.embedding.{EmbeddingContextLengthExceeded, EmbeddingGenerationFailed, EmbeddingService}
import repcheck.pipeline.models.metadata.ProcessingResult
import repcheck.shared.models.congress.dos.amendment.AmendmentTextChunkDO

/**
 * Unit specs for the foreground-only [[CrossAmendmentEmbedder]]. Mirror of the bill-side spec; covers single- amendment
 * happy path, cross-amendment batching, empty-stream, and failure modes.
 */
class CrossAmendmentEmbedderSpec extends AnyFlatSpec with Matchers {

  private val testXa: Transactor[IO] = Transactor.fromDriverManager[IO](
    driver = "org.h2.Driver",
    url = "jdbc:h2:mem:cross-amend-embedder-spec;DB_CLOSE_DELAY=-1",
    user = "",
    password = "",
    logHandler = None,
  )

  private val testLogger = new PipelineLogger[IO] {
    override def info(context: LogContext, message: String): IO[Unit]                            = IO.unit
    override def warn(context: LogContext, message: String): IO[Unit]                            = IO.unit
    override def error(context: LogContext, message: String, cause: Option[Throwable]): IO[Unit] = IO.unit
    override def debug(context: LogContext, message: String): IO[Unit]                           = IO.unit
  }

  private val TestTimeout: FiniteDuration = 2.seconds

  private class RecordingEmbeddingService extends EmbeddingService[IO] {
    private val batchesRef = new AtomicReference[Vector[List[String]]](Vector.empty)

    override def generateEmbedding(text: String): IO[Option[Array[Float]]] =
      generateEmbeddings(List(text)).map(_.headOption.flatten)

    override def generateEmbeddings(texts: List[String]): IO[List[Option[Array[Float]]]] = IO {
      val _ = batchesRef.updateAndGet(prev => prev :+ texts)
      texts.map(t => Some(Array(t.length.toFloat, 0.0f, 0.0f, 0.0f)))
    }

    def batches: Vector[List[String]] = batchesRef.get()
  }

  private class FailingEmbeddingService(error: Throwable) extends EmbeddingService[IO] {
    override def generateEmbedding(text: String): IO[Option[Array[Float]]] = IO.raiseError(error)

    override def generateEmbeddings(texts: List[String]): IO[List[Option[Array[Float]]]] =
      IO.raiseError(error)

  }

  private class RecordingRepo extends AmendmentTextChunkRepository[ConnectionIO] {
    private val rowsRef = new AtomicReference[Vector[List[AmendmentTextChunkDO]]](Vector.empty)

    override def insertMany(rows: List[AmendmentTextChunkDO]): ConnectionIO[Unit] =
      doobie.free.connection.delay {
        val _ = rowsRef.updateAndGet(prev => prev :+ rows)
      }

    override def deleteByVersionId(versionId: Long): ConnectionIO[Unit] = doobie.free.connection.unit
    override def countByVersionId(versionId: Long): ConnectionIO[Long]  = doobie.free.connection.pure(0L)

    override def sumContentLengthByVersionId(versionId: Long): ConnectionIO[Long] =
      doobie.free.connection.pure(0L)

    override def findByVersionId(versionId: Long): ConnectionIO[List[AmendmentTextChunkDO]] =
      doobie.free.connection.pure(List.empty)

    def rows: Vector[List[AmendmentTextChunkDO]] = rowsRef.get()
  }

  private val testCtx  = AmendmentEmbedCtx(amendmentId = 42L, versionId = 7L, naturalKey = "117-SAMDT-2137")
  private val testCtx2 = AmendmentEmbedCtx(amendmentId = 99L, versionId = 8L, naturalKey = "117-HAMDT-3")

  "processChunks" should "succeed for a single chunk via finalize-flush" in {
    val embedSvc = new RecordingEmbeddingService
    val repo     = new RecordingRepo
    val program = CrossAmendmentEmbedder
      .resource[IO](embedSvc, repo, testXa, testLogger, batchSize = 50)
      .use(embedder => embedder.processChunks(testCtx, Stream.emit("only chunk")))
      .timeout(TestTimeout)

    val result = program.unsafeRunSync()
    val _ = result match {
      case ProcessingResult.Succeeded(naturalKey, _) => naturalKey shouldBe "117-SAMDT-2137"
      case other                                     => fail(s"Expected Succeeded, got $other")
    }
    val _ = embedSvc.batches.size shouldBe 1
    val _ = repo.rows.size shouldBe 1
    repo.rows.headOption.flatMap(_.headOption).map(_.amendmentId) shouldBe Some(42L)
  }

  it should "succeed for an empty chunk stream (Deferred resolves immediately at finalize)" in {
    val embedSvc = new RecordingEmbeddingService
    val repo     = new RecordingRepo
    val result = CrossAmendmentEmbedder
      .resource[IO](embedSvc, repo, testXa, testLogger, batchSize = 50)
      .use(embedder => embedder.processChunks(testCtx, Stream.empty))
      .timeout(TestTimeout)
      .unsafeRunSync()

    val _ = result.isSucceeded shouldBe true
    val _ = embedSvc.batches shouldBe empty
    repo.rows shouldBe empty
  }

  it should "process multiple chunks for the same amendment in one finalize-flush when batchSize is large" in {
    val embedSvc = new RecordingEmbeddingService
    val repo     = new RecordingRepo
    val result = CrossAmendmentEmbedder
      .resource[IO](embedSvc, repo, testXa, testLogger, batchSize = 50)
      .use(embedder => embedder.processChunks(testCtx, Stream("chunk-0", "chunk-1", "chunk-2")))
      .timeout(TestTimeout)
      .unsafeRunSync()

    val _ = result.isSucceeded shouldBe true
    val _ = embedSvc.batches.size shouldBe 1
    val _ = embedSvc.batches.headOption.map(_.size) shouldBe Some(3)
    repo.rows.flatten should have size 3
  }

  it should "preserve chunk_index and amendmentId on persisted rows" in {
    val embedSvc = new RecordingEmbeddingService
    val repo     = new RecordingRepo
    val _ = CrossAmendmentEmbedder
      .resource[IO](embedSvc, repo, testXa, testLogger, batchSize = 50)
      .use(embedder => embedder.processChunks(testCtx, Stream("a", "b")))
      .timeout(TestTimeout)
      .unsafeRunSync()

    val rows = repo.rows.flatten
    val _    = rows.size shouldBe 2
    val _    = rows.map(_.amendmentId).distinct shouldBe List(42L)
    val _    = rows.map(_.versionId.getOrElse(-1L)).distinct shouldBe List(7L)
    rows.map(_.chunkIndex).sorted shouldBe List(0, 1)
  }

  it should "Fail with Transient when the embedding service raises a network error" in {
    val embedSvc = new FailingEmbeddingService(new java.net.SocketTimeoutException("network timeout"))
    val repo     = new RecordingRepo
    val result = CrossAmendmentEmbedder
      .resource[IO](embedSvc, repo, testXa, testLogger, batchSize = 50)
      .use(embedder => embedder.processChunks(testCtx, Stream.emit("chunk")))
      .timeout(TestTimeout)
      .unsafeRunSync()
    result match {
      case ProcessingResult.Failed(_, _, errorClass) => errorClass shouldBe "Transient"
      case other                                     => fail(s"Expected Failed(Transient), got $other")
    }
  }

  it should "Fail with Systemic when the embedding service raises an EmbeddingContextLengthExceeded" in {
    val embedSvc = new FailingEmbeddingService(EmbeddingContextLengthExceeded("too big", 100000))
    val repo     = new RecordingRepo
    val result = CrossAmendmentEmbedder
      .resource[IO](embedSvc, repo, testXa, testLogger, batchSize = 50)
      .use(embedder => embedder.processChunks(testCtx, Stream.emit("oversized chunk")))
      .timeout(TestTimeout)
      .unsafeRunSync()
    result match {
      case ProcessingResult.Failed(_, _, errorClass) => errorClass shouldBe "Systemic"
      case other                                     => fail(s"Expected Failed(Systemic), got $other")
    }
  }

  it should "Fail with Transient on EmbeddingGenerationFailed" in {
    val embedSvc = new FailingEmbeddingService(EmbeddingGenerationFailed("ollama hiccup", 100))
    val repo     = new RecordingRepo
    val result = CrossAmendmentEmbedder
      .resource[IO](embedSvc, repo, testXa, testLogger, batchSize = 50)
      .use(embedder => embedder.processChunks(testCtx, Stream.emit("c")))
      .timeout(TestTimeout)
      .unsafeRunSync()
    result match {
      case ProcessingResult.Failed(_, _, errorClass) => errorClass shouldBe "Transient"
      case other                                     => fail(s"Expected Failed(Transient), got $other")
    }
  }

  it should "complete two amendments via cross-amendment batching when batchSize triggers a shared flush" in {
    val embedSvc = new RecordingEmbeddingService
    val repo     = new RecordingRepo
    // batchSize=2 → first amendment's 1 chunk fills slot 1, second amendment's 1 chunk triggers the flush.
    val program = CrossAmendmentEmbedder
      .resource[IO](embedSvc, repo, testXa, testLogger, batchSize = 2)
      .use { embedder =>
        for {
          fiber1 <- embedder.processChunks(testCtx, Stream.emit("amend-1-chunk")).start
          fiber2 <- embedder.processChunks(testCtx2, Stream.emit("amend-2-chunk")).start
          r1     <- fiber1.joinWithNever
          r2     <- fiber2.joinWithNever
        } yield (r1, r2)
      }
      .timeout(TestTimeout)
      .unsafeRunSync()

    val (r1, r2) = program
    val _        = r1.isSucceeded shouldBe true
    val _        = r2.isSucceeded shouldBe true
    val _        = repo.rows.flatten.size shouldBe 2
    repo.rows.flatten.map(_.amendmentId).toSet shouldBe Set(42L, 99L)
  }

}
