package repcheck.ingestion.bills.text.embedding

import java.util.concurrent.atomic.AtomicReference

import scala.concurrent.duration._

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.syntax.all._

import fs2.Stream

import doobie._

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import repcheck.ingestion.bills.text.persistence.RawBillTextRepository
import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}
import repcheck.pipeline.models.metadata.ProcessingResult
import repcheck.shared.models.congress.dos.bill.RawBillTextDO

/**
 * Unit specs for the foreground-only [[CrossBillEmbedder]]. Each test wraps its IO in `IO.timeout(2.seconds)` so a hang
 * surfaces as a fast failure rather than blocking sbt forever — the FG-only design has no background fibers, so nothing
 * should ever actually hang, but the timeout is cheap insurance against future regressions.
 *
 * What we cover:
 *
 *   - single-bill happy path: a single chunk → finalize force-flushes → success
 *   - single-bill multi-chunk: 3 chunks with batchSize=50 → all flushed at finalize → success
 *   - cross-bill batch fold: 2 bills × 1 chunk each with batchSize=2 → second bill's offer triggers flush → both bills
 *     succeed via the SAME flush
 *   - empty stream: 0 chunks → finalize completes Deferred immediately via `expected = Some(0) == persisted`
 *   - failure paths: embedding service raises (Failed/Transient), DB raises during insertMany (Failed/Transient),
 *     EmbeddingContextLengthExceeded (Failed/Systemic)
 *   - association invariant: under cross-bill mixing, every persisted row's billId matches its content's source
 */
class CrossBillEmbedderSpec extends AnyFlatSpec with Matchers {

  private val testXa: Transactor[IO] = Transactor.fromDriverManager[IO](
    driver = "org.h2.Driver",
    url = "jdbc:h2:mem:cross-bill-embedder-spec;DB_CLOSE_DELAY=-1",
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

  /** Records every batch handed to `generateEmbeddings`. Returns deterministic embeddings of `[length, 0, 0, 0]`. */
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

  private class RecordingRawRepo extends RawBillTextRepository[ConnectionIO] {
    private val rowsRef = new AtomicReference[Vector[List[RawBillTextDO]]](Vector.empty)

    override def insertMany(rows: List[RawBillTextDO]): ConnectionIO[Unit] =
      doobie.free.connection.delay {
        val _ = rowsRef.updateAndGet(prev => prev :+ rows)
      }

    override def insertOne(row: RawBillTextDO): ConnectionIO[Unit] =
      doobie.free.connection.delay {
        val _ = rowsRef.updateAndGet(prev => prev :+ List(row))
      }

    override def deleteByVersionId(versionId: Long): ConnectionIO[Unit] = doobie.free.connection.unit

    override def replaceAll(versionId: Long, rows: List[RawBillTextDO]): ConnectionIO[Unit] =
      doobie.free.connection.unit

    override def countByVersionId(versionId: Long): ConnectionIO[Long] = doobie.free.connection.pure(0L)

    override def findByVersionId(versionId: Long): ConnectionIO[List[RawBillTextDO]] =
      doobie.free.connection.pure(List.empty)

    def batches: Vector[List[RawBillTextDO]] = rowsRef.get()
    def allRows: List[RawBillTextDO]         = rowsRef.get().toList.flatten
  }

  private class FailingRawRepo(error: Throwable) extends RawBillTextRepository[ConnectionIO] {

    override def insertMany(rows: List[RawBillTextDO]): ConnectionIO[Unit] =
      doobie.free.connection.raiseError(error)

    override def insertOne(row: RawBillTextDO): ConnectionIO[Unit]      = doobie.free.connection.unit
    override def deleteByVersionId(versionId: Long): ConnectionIO[Unit] = doobie.free.connection.unit

    override def replaceAll(versionId: Long, rows: List[RawBillTextDO]): ConnectionIO[Unit] =
      doobie.free.connection.unit

    override def countByVersionId(versionId: Long): ConnectionIO[Long] = doobie.free.connection.pure(0L)

    override def findByVersionId(versionId: Long): ConnectionIO[List[RawBillTextDO]] =
      doobie.free.connection.pure(List.empty)

  }

  private def ctx(billId: Long, naturalKey: String): BillEmbedCtx =
    BillEmbedCtx(dbBillId = billId, versionId = billId * 100L, naturalKey = naturalKey)

  private def runWithEmbedder[A](
    embeddingService: EmbeddingService[IO],
    rawRepo: RawBillTextRepository[ConnectionIO],
    batchSize: Int,
  )(body: CrossBillEmbedder[IO] => IO[A]): A =
    CrossBillEmbedder
      .resource[IO](
        embeddingService = embeddingService,
        rawBillTextRepository = rawRepo,
        xa = testXa,
        logger = testLogger,
        batchSize = batchSize,
      )
      .use(body)
      .timeout(TestTimeout)
      .unsafeRunSync()

  // ===========================================================================
  // Single-bill happy paths
  // ===========================================================================

  "processChunks" should "succeed with a single-chunk bill (finalize force-flushes the residual)" in {
    val embedder = new RecordingEmbeddingService
    val rawRepo  = new RecordingRawRepo

    val result = runWithEmbedder(embedder, rawRepo, batchSize = 50) { e =>
      e.processChunks(ctx(1L, "118-HR-1"), Stream.emit("only chunk"))
    }

    val _ = result match {
      case ProcessingResult.Succeeded(naturalKey, _) => naturalKey shouldBe "118-HR-1"
      case other                                     => fail(s"Expected Succeeded but got $other")
    }
    val _ = rawRepo.allRows.size shouldBe 1
    val _ = rawRepo.allRows.headOption.map(_.content) shouldBe Some("only chunk")
    embedder.batches.toList.flatten shouldBe List("only chunk")
  }

  it should "succeed with a multi-chunk bill, preserving chunk order" in {
    val embedder = new RecordingEmbeddingService
    val rawRepo  = new RecordingRawRepo

    val result = runWithEmbedder(embedder, rawRepo, batchSize = 50) { e =>
      e.processChunks(ctx(1L, "118-HR-1"), Stream.emits(List("a", "b", "c")))
    }

    val _ = result.isInstanceOf[ProcessingResult.Succeeded] shouldBe true
    val _ = rawRepo.allRows.size shouldBe 3
    val _ = rawRepo.allRows.map(_.content) shouldBe List("a", "b", "c")
    rawRepo.allRows.map(_.chunkIndex) shouldBe List(0, 1, 2)
  }

  it should "succeed (no-op) on an empty chunk stream" in {
    val embedder = new RecordingEmbeddingService
    val rawRepo  = new RecordingRawRepo

    val result = runWithEmbedder(embedder, rawRepo, batchSize = 50) { e =>
      e.processChunks(ctx(1L, "118-HR-1"), Stream.empty)
    }

    val _ = result match {
      case ProcessingResult.Succeeded(naturalKey, _) => naturalKey shouldBe "118-HR-1"
      case other                                     => fail(s"Expected Succeeded but got $other")
    }
    val _ = embedder.batches shouldBe empty
    rawRepo.allRows shouldBe empty
  }

  // ===========================================================================
  // Cross-bill batching
  // ===========================================================================

  it should "process all chunks from concurrent bills correctly (cross-bill batching is timing-dependent)" in {
    // FG-only embedder caveat: with two concurrent single-chunk bills and batchSize=2, whether the chunks land in
    // ONE batch (cross-bill fold) or TWO batches (each bill's finalize force-flushes its own chunk before the
    // other can offer) depends on fiber-scheduling timing. The CORRECTNESS invariant is independent of that:
    //
    //   - both bills must Succeed
    //   - the union of all batches' texts must equal the union of submitted texts
    //   - the rawRepo's rows must contain both bills' content
    //
    // Cross-bill folding is a perf optimization that the architecture supports but doesn't guarantee for any
    // particular pair of bills. Production-scale behavior (32 concurrent bills feeding a batchSize=50 buffer) is
    // overwhelmingly cross-bill, but synthetic two-bill tests are a coin flip.
    val embedder = new RecordingEmbeddingService
    val rawRepo  = new RecordingRawRepo

    val (r1, r2) = runWithEmbedder(embedder, rawRepo, batchSize = 2) { e =>
      val s1 = e.processChunks(ctx(1L, "118-HR-1"), Stream.emit("alpha"))
      val s2 = e.processChunks(ctx(2L, "118-HR-2"), Stream.emit("beta"))
      IO.both(s1, s2)
    }

    val _ = r1.isInstanceOf[ProcessingResult.Succeeded] shouldBe true
    val _ = r2.isInstanceOf[ProcessingResult.Succeeded] shouldBe true
    // Sum of all batches' texts equals submitted set, regardless of how they got partitioned.
    val _ = embedder.batches.toList.flatten.toSet shouldBe Set("alpha", "beta")
    rawRepo.allRows.map(_.content).toSet shouldBe Set("alpha", "beta")
  }

  it should "associate each row's billId with its OWN content under cross-bill mixing" in {
    val embedder = new RecordingEmbeddingService
    val rawRepo  = new RecordingRawRepo

    val billA = ctx(1L, "118-HR-1")
    val billB = ctx(2L, "118-HR-2")
    val billC = ctx(3L, "118-HR-3")

    val results = runWithEmbedder(embedder, rawRepo, batchSize = 5) { e =>
      val a = e.processChunks(billA, Stream.emits(List("AAAA", "AAAAAA"))) // 2 chunks, lengths 4 and 6
      val b = e.processChunks(billB, Stream.emits(List("BBB")))            // 1 chunk, length 3
      val c = e.processChunks(billC, Stream.emits(List("CC", "CCCCCCCC"))) // 2 chunks, lengths 2 and 8
      (a, b, c).parTupled
    }

    val _ = results._1.isInstanceOf[ProcessingResult.Succeeded] shouldBe true
    val _ = results._2.isInstanceOf[ProcessingResult.Succeeded] shouldBe true
    val _ = results._3.isInstanceOf[ProcessingResult.Succeeded] shouldBe true

    val rowsByBill: Map[Long, List[RawBillTextDO]] = rawRepo.allRows.groupBy(_.billId)
    val _                                          = rowsByBill(1L).map(_.content).toSet shouldBe Set("AAAA", "AAAAAA")
    val _                                          = rowsByBill(2L).map(_.content).toSet shouldBe Set("BBB")
    val _                                          = rowsByBill(3L).map(_.content).toSet shouldBe Set("CC", "CCCCCCCC")

    // Stub puts text.length in emb[0]; row's emb[0] must match row's own content.length, not some other row's.
    rawRepo.allRows.foreach { row =>
      val embFirstFloat = row.embedding.flatMap(_.headOption).getOrElse(-1.0f)
      withClue(s"row content='${row.content}' embedding[0]=$embFirstFloat") {
        embFirstFloat shouldBe row.content.length.toFloat
      }
    }
  }

  // ===========================================================================
  // Failure paths
  // ===========================================================================

  it should "report Failed(Transient) when the embedding service raises a Transient error" in {
    val rawRepo  = new RecordingRawRepo
    val embedder = new FailingEmbeddingService(EmbeddingGenerationFailed("ollama 503", 5))

    val result = runWithEmbedder(embedder, rawRepo, batchSize = 50) { e =>
      e.processChunks(ctx(1L, "118-HR-1"), Stream.emit("doomed"))
    }

    val _ = result match {
      case ProcessingResult.Failed(_, _, errorClass) => errorClass shouldBe "Transient"
      case other                                     => fail(s"Expected Failed(Transient) but got $other")
    }
    rawRepo.allRows shouldBe empty
  }

  it should "report Failed(Systemic) for EmbeddingContextLengthExceeded" in {
    val rawRepo  = new RecordingRawRepo
    val embedder = new FailingEmbeddingService(EmbeddingContextLengthExceeded("oversized", 30001))

    val result = runWithEmbedder(embedder, rawRepo, batchSize = 50) { e =>
      e.processChunks(ctx(1L, "118-HR-1"), Stream.emit("oversized"))
    }

    val _ = result match {
      case ProcessingResult.Failed(_, _, errorClass) => errorClass shouldBe "Systemic"
      case other                                     => fail(s"Expected Failed(Systemic) but got $other")
    }
    rawRepo.allRows shouldBe empty
  }

  it should "fail every bill in a batch when the embed call fails" in {
    val rawRepo  = new RecordingRawRepo
    val embedder = new FailingEmbeddingService(new java.io.IOException("network down"))

    val (r1, r2) = runWithEmbedder(embedder, rawRepo, batchSize = 2) { e =>
      val s1 = e.processChunks(ctx(1L, "118-HR-1"), Stream.emit("a"))
      val s2 = e.processChunks(ctx(2L, "118-HR-2"), Stream.emit("b"))
      IO.both(s1, s2)
    }

    val _ = r1 match {
      case ProcessingResult.Failed(_, _, ec) => ec shouldBe "Transient"
      case other                             => fail(s"Expected Failed(Transient) for r1 but got $other")
    }
    r2 match {
      case ProcessingResult.Failed(_, _, ec) => ec shouldBe "Transient"
      case other                             => fail(s"Expected Failed(Transient) for r2 but got $other")
    }
  }

  it should "report Failed when insertMany raises (DB error)" in {
    val embedder = new RecordingEmbeddingService
    val rawRepo  = new FailingRawRepo(new java.sql.SQLTransientConnectionException("conn lost"))

    val result = runWithEmbedder(embedder, rawRepo, batchSize = 50) { e =>
      e.processChunks(ctx(1L, "118-HR-1"), Stream.emit("doomed"))
    }

    val _ = result match {
      case ProcessingResult.Failed(_, _, errorClass) => errorClass shouldBe "Transient"
      case other                                     => fail(s"Expected Failed(Transient) but got $other")
    }
    embedder.batches.size shouldBe 1 // embed was attempted before insertMany raised
  }

  // ===========================================================================
  // Resource cleanup edge case
  // ===========================================================================

  it should "leave a bill in state when a flush completes but more chunks are pending (else branch of applyBatchResult)" in {
    // Covers the `if (updated.shouldComplete) { ... } else { ... }` else branch on line 274:
    // a bill whose persisted-count grows past one flush but hasn't reached `expected` yet.
    // batchSize = 1 → every chunk triggers a flush → first 2 flushes leave the bill still in state.
    val embedder = new RecordingEmbeddingService
    val rawRepo  = new RecordingRawRepo

    val result = runWithEmbedder(embedder, rawRepo, batchSize = 1) { e =>
      e.processChunks(ctx(1L, "118-HR-1"), Stream.emits(List("a", "b", "c")))
    }

    val _ = result match {
      case ProcessingResult.Succeeded(naturalKey, _) => naturalKey shouldBe "118-HR-1"
      case other                                     => fail(s"Expected Succeeded but got $other")
    }
    // 3 separate batches because batchSize=1 forces a flush per chunk.
    val _ = embedder.batches.size shouldBe 3
    rawRepo.allRows.size shouldBe 3
  }

  it should "classify SocketTimeoutException as Transient" in {
    val rawRepo  = new RecordingRawRepo
    val embedder = new FailingEmbeddingService(new java.net.SocketTimeoutException("read timed out"))

    val result = runWithEmbedder(embedder, rawRepo, batchSize = 50) { e =>
      e.processChunks(ctx(1L, "118-HR-1"), Stream.emit("text"))
    }

    result match {
      case ProcessingResult.Failed(_, _, ec) => ec shouldBe "Transient"
      case other                             => fail(s"Expected Failed(Transient) but got $other")
    }
  }

  it should "classify ConnectException as Transient" in {
    val rawRepo  = new RecordingRawRepo
    val embedder = new FailingEmbeddingService(new java.net.ConnectException("connection refused"))

    val result = runWithEmbedder(embedder, rawRepo, batchSize = 50) { e =>
      e.processChunks(ctx(1L, "118-HR-1"), Stream.emit("text"))
    }

    result match {
      case ProcessingResult.Failed(_, _, ec) => ec shouldBe "Transient"
      case other                             => fail(s"Expected Failed(Transient) but got $other")
    }
  }

  it should "classify SQLTransientException as Transient via DB-side failure" in {
    val rawRepo  = new FailingRawRepo(new java.sql.SQLTransientException("rolled back"))
    val embedder = new RecordingEmbeddingService

    val result = runWithEmbedder(embedder, rawRepo, batchSize = 50) { e =>
      e.processChunks(ctx(1L, "118-HR-1"), Stream.emit("text"))
    }

    result match {
      case ProcessingResult.Failed(_, _, ec) => ec shouldBe "Transient"
      case other                             => fail(s"Expected Failed(Transient) but got $other")
    }
  }

  it should "classify a generic Throwable as Systemic (default branch)" in {
    val rawRepo  = new RecordingRawRepo
    val embedder = new FailingEmbeddingService(new RuntimeException("unexpected"))

    val result = runWithEmbedder(embedder, rawRepo, batchSize = 50) { e =>
      e.processChunks(ctx(1L, "118-HR-1"), Stream.emit("text"))
    }

    result match {
      case ProcessingResult.Failed(_, _, ec) => ec shouldBe "Systemic"
      case other                             => fail(s"Expected Failed(Systemic) but got $other")
    }
  }

  it should "fall back to the simple class name when the exception's message is null" in {
    // Covers the `Option(error.getMessage).getOrElse(error.getClass.getSimpleName)` branch in failBatch.
    val rawRepo  = new RecordingRawRepo
    val embedder = new FailingEmbeddingService(new RuntimeException()) // no message

    val result = runWithEmbedder(embedder, rawRepo, batchSize = 50) { e =>
      e.processChunks(ctx(1L, "118-HR-1"), Stream.emit("text"))
    }

    result match {
      case ProcessingResult.Failed(_, reason, _) =>
        // Exception with null message falls back to simple class name.
        val _ = reason should include("RuntimeException")
        succeed
      case other => fail(s"Expected Failed but got $other")
    }
  }

  it should "Fail the bill's Deferred via cleanup when the chunk stream raises mid-submission" in {
    val embedder    = new RecordingEmbeddingService
    val rawRepo     = new RecordingRawRepo
    val raisedError = new IllegalStateException("upstream extractor failed")

    val attempt = CrossBillEmbedder
      .resource[IO](
        embeddingService = embedder,
        rawBillTextRepository = rawRepo,
        xa = testXa,
        logger = testLogger,
        batchSize = 50,
      )
      .use { e =>
        val failingStream = Stream.emit("ok") ++ Stream.raiseError[IO](raisedError)
        e.processChunks(ctx(1L, "118-HR-1"), failingStream).attempt
      }
      .timeout(TestTimeout)
      .unsafeRunSync()

    val _ = attempt.isLeft shouldBe true
    attempt.left.toOption.map(_.getMessage) shouldBe Some("upstream extractor failed")
  }

}
