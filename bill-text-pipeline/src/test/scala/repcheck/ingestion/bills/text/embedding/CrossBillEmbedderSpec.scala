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
 * Unit specs for [[CrossBillEmbedder]]. Tests the cross-bill batching behavior end-to-end without spinning up a real
 * Ollama server or AlloyDB. To keep these fast (and to make a hang surface as a fast failure rather than a timeout),
 * every test wraps its IO in `IO.timeout(2.seconds)` — the embedder's worker fiber runs sub-second batches with the
 * test timeouts (`embedBatchTimeout = 50.millis`), so 2s is generous and a real bug shows up immediately.
 *
 * Concurrency model: tests that need to submit chunks for multiple bills concurrently use `IO.both`, which runs two IOs
 * in parallel and returns when BOTH complete. The embedder's worker fiber drains the queue across both bills, the test
 * gets two completion results, and the Resource closes cleanly.
 *
 * What's intentionally NOT tested here (lives in the integration specs that DO use real Docker AlloyDB + Ollama):
 *
 *   - real GPU saturation curves
 *   - vector serialization (pgvector)
 *   - real Pub/Sub redelivery on process restart
 */
class CrossBillEmbedderSpec extends AnyFlatSpec with Matchers {

  // H2 in-memory; just needs a connection to commit empty Doobie programs. Re-using the same DB across tests is fine
  // because every insertMany is a `connection.delay { ... }` that touches our Vector ref, not real tables.
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

  private val TestTimeout: FiniteDuration      = 2.seconds
  private val FastBatchTimeout: FiniteDuration = 50.millis

  /** EmbeddingService stub that records every batch it sees and returns deterministic embeddings. */
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

  /** EmbeddingService stub that always raises. */
  private class FailingEmbeddingService(error: Throwable) extends EmbeddingService[IO] {
    override def generateEmbedding(text: String): IO[Option[Array[Float]]] = IO.raiseError(error)

    override def generateEmbeddings(texts: List[String]): IO[List[Option[Array[Float]]]] =
      IO.raiseError(error)

  }

  /** RawBillTextRepository stub that records every batch handed to insertMany. */
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

  /** RawBillTextRepository stub that always raises on insertMany. */
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

  /**
   * Allocate an embedder with the given collaborators, run `body` inside its Resource, wrap the whole thing in a 2s
   * IO.timeout. If the test hangs (e.g., a Deferred never resolves), the timeout makes it surface as a
   * `TimeoutException` rather than blocking sbt forever.
   */
  private def runWithEmbedder[A](
    embeddingService: EmbeddingService[IO],
    rawRepo: RawBillTextRepository[ConnectionIO],
    embedBatchSize: Int,
    embedBatchTimeout: FiniteDuration = FastBatchTimeout,
    queueCapacity: Int = 50,
  )(body: CrossBillEmbedder[IO] => IO[A]): A =
    CrossBillEmbedder
      .resource[IO](
        embeddingService = embeddingService,
        rawBillTextRepository = rawRepo,
        xa = testXa,
        logger = testLogger,
        embedBatchSize = embedBatchSize,
        embedBatchTimeout = embedBatchTimeout,
        queueCapacity = queueCapacity,
      )
      .use(body)
      .timeout(TestTimeout)
      .unsafeRunSync()

  // ===========================================================================
  // Single-bill happy path
  // ===========================================================================

  "processChunks" should "succeed with a single-chunk single-bill submission" in {
    val embedder = new RecordingEmbeddingService
    val rawRepo  = new RecordingRawRepo

    val result = runWithEmbedder(embedder, rawRepo, embedBatchSize = 50) { e =>
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

  it should "succeed with a multi-chunk single-bill submission preserving chunk order" in {
    val embedder = new RecordingEmbeddingService
    val rawRepo  = new RecordingRawRepo

    val result = runWithEmbedder(embedder, rawRepo, embedBatchSize = 50) { e =>
      e.processChunks(ctx(1L, "118-HR-1"), Stream.emits(List("a", "b", "c")))
    }

    val _ = result.isInstanceOf[ProcessingResult.Succeeded] shouldBe true
    val _ = rawRepo.allRows.size shouldBe 3
    val _ = rawRepo.allRows.map(_.content) shouldBe List("a", "b", "c")
    rawRepo.allRows.map(_.chunkIndex) shouldBe List(0, 1, 2)
  }

  // ===========================================================================
  // Cross-bill batching — the whole point of the refactor
  // ===========================================================================

  it should "fold chunks from multiple concurrent bills into one Ollama batch" in {
    // Submit 2 bills, each with 1 chunk. With embedBatchSize=2 and a long timeout, both chunks should be packed
    // into ONE batch — the GPU sees 2 texts in one call instead of two single-text calls.
    val embedder = new RecordingEmbeddingService
    val rawRepo  = new RecordingRawRepo

    val (r1, r2) = runWithEmbedder(embedder, rawRepo, embedBatchSize = 2, embedBatchTimeout = 1.second) { e =>
      val s1 = e.processChunks(ctx(1L, "118-HR-1"), Stream.emit("alpha"))
      val s2 = e.processChunks(ctx(2L, "118-HR-2"), Stream.emit("beta"))
      IO.both(s1, s2)
    }

    val _ = r1.isInstanceOf[ProcessingResult.Succeeded] shouldBe true
    val _ = r2.isInstanceOf[ProcessingResult.Succeeded] shouldBe true
    // The chunks were folded into ONE Ollama batch — assert exactly 1 batch with 2 texts.
    val _ = embedder.batches.size shouldBe 1
    embedder.batches.headOption.map(_.toSet) shouldBe Some(Set("alpha", "beta"))
  }

  it should "associate each row's billId with its OWN content (no cross-bill mixup under cross-batching)" in {
    // The correctness invariant: if bill A and bill B share a batch, bill A's row must have content="A's text"
    // AND billId=A, never "A's text" + billId=B. The RecordingEmbeddingService returns embeddings of length =
    // text.length as float[0] — so we can later verify embedding↔text consistency too.
    val embedder = new RecordingEmbeddingService
    val rawRepo  = new RecordingRawRepo

    val billA = ctx(1L, "118-HR-1")
    val billB = ctx(2L, "118-HR-2")
    val billC = ctx(3L, "118-HR-3")

    val results = runWithEmbedder(embedder, rawRepo, embedBatchSize = 5, embedBatchTimeout = 1.second) { e =>
      val a = e.processChunks(billA, Stream.emits(List("AAAA", "AAAAAA"))) // 2 chunks, lengths 4 and 6
      val b = e.processChunks(billB, Stream.emits(List("BBB")))            // 1 chunk, length 3
      val c = e.processChunks(billC, Stream.emits(List("CC", "CCCCCCCC"))) // 2 chunks, lengths 2 and 8
      // All 5 chunks should land in ONE batch (batchSize=5).
      (a, b, c).parTupled
    }

    val _ = results._1.isInstanceOf[ProcessingResult.Succeeded] shouldBe true
    val _ = results._2.isInstanceOf[ProcessingResult.Succeeded] shouldBe true
    val _ = results._3.isInstanceOf[ProcessingResult.Succeeded] shouldBe true

    // Exactly one cross-bill batch was emitted.
    val _ = embedder.batches.size shouldBe 1

    // Now the critical assertions: every row's content must match its billId's expected texts, and the
    // embedding's first float (which the stub sets to text.length) must match the row's content.length.
    val rowsByBill: Map[Long, List[RawBillTextDO]] = rawRepo.allRows.groupBy(_.billId)

    val _ = rowsByBill(1L).map(_.content).toSet shouldBe Set("AAAA", "AAAAAA")
    val _ = rowsByBill(2L).map(_.content).toSet shouldBe Set("BBB")
    val _ = rowsByBill(3L).map(_.content).toSet shouldBe Set("CC", "CCCCCCCC")

    // Per-row embedding↔content consistency: the stub puts text.length into emb[0], so for every row, that
    // embedding must encode its OWN content's length, not some other bill's. This is the strongest check —
    // it would catch a bug where the embedder zipped batch.toList with embeddings in a different order.
    rawRepo.allRows.foreach { row =>
      val embFirstFloat = row.embedding.flatMap(_.headOption).getOrElse(-1.0f)
      withClue(s"row content='${row.content}' embedding[0]=$embFirstFloat") {
        embFirstFloat shouldBe row.content.length.toFloat
      }
    }

    // Per-row versionId ↔ billId consistency: ctx has versionId = billId * 100, so rows for billId=N must
    // all have versionId=N*100.
    rawRepo.allRows.foreach { row =>
      withClue(s"row billId=${row.billId} versionId=${row.versionId}") {
        row.versionId shouldBe Some(row.billId * 100L)
      }
    }
  }

  it should "fire on timeout when fewer chunks than batchSize arrive" in {
    // batchSize=10 but only 1 chunk submitted; the 50ms timeout should force the worker to flush.
    val embedder = new RecordingEmbeddingService
    val rawRepo  = new RecordingRawRepo

    val result = runWithEmbedder(embedder, rawRepo, embedBatchSize = 10) { e =>
      e.processChunks(ctx(7L, "118-HR-7"), Stream.emit("solo"))
    }

    val _ = result.isInstanceOf[ProcessingResult.Succeeded] shouldBe true
    embedder.batches.toList.flatten shouldBe List("solo")
  }

  // ===========================================================================
  // Failure paths — embedding service raises
  // ===========================================================================

  it should "report Failed(Transient) when the embedding service raises a Transient error" in {
    val rawRepo  = new RecordingRawRepo
    val embedder = new FailingEmbeddingService(EmbeddingGenerationFailed("ollama 503", 5))

    val result = runWithEmbedder(embedder, rawRepo, embedBatchSize = 50) { e =>
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

    val result = runWithEmbedder(embedder, rawRepo, embedBatchSize = 50) { e =>
      e.processChunks(ctx(1L, "118-HR-1"), Stream.emit("oversized"))
    }

    val _ = result match {
      case ProcessingResult.Failed(_, _, errorClass) => errorClass shouldBe "Systemic"
      case other                                     => fail(s"Expected Failed(Systemic) but got $other")
    }
    rawRepo.allRows shouldBe empty
  }

  it should "report Failed for ALL bills in a failing batch" in {
    // batchSize=2 forces both bills into one batch; failing the batch must Fail BOTH bills' Deferreds.
    val rawRepo  = new RecordingRawRepo
    val embedder = new FailingEmbeddingService(new java.io.IOException("network down"))

    val (r1, r2) = runWithEmbedder(embedder, rawRepo, embedBatchSize = 2, embedBatchTimeout = 1.second) { e =>
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

  // ===========================================================================
  // Failure paths — DB raises during insertMany
  // ===========================================================================

  it should "report Failed when insertMany raises" in {
    val embedder = new RecordingEmbeddingService
    val rawRepo  = new FailingRawRepo(new java.sql.SQLTransientConnectionException("conn lost"))

    val result = runWithEmbedder(embedder, rawRepo, embedBatchSize = 50) { e =>
      e.processChunks(ctx(1L, "118-HR-1"), Stream.emit("doomed"))
    }

    val _ = result match {
      case ProcessingResult.Failed(_, _, errorClass) => errorClass shouldBe "Transient"
      case other                                     => fail(s"Expected Failed(Transient) but got $other")
    }
    embedder.batches.size shouldBe 1 // embedding service WAS called before the DB write failed
  }

  // ===========================================================================
  // Edge cases
  // ===========================================================================

  it should "succeed (no-op) when the chunk stream is empty" in {
    // expected becomes 0; persisted starts at 0 → shouldComplete is true immediately on finalize.
    val embedder = new RecordingEmbeddingService
    val rawRepo  = new RecordingRawRepo

    val result = runWithEmbedder(embedder, rawRepo, embedBatchSize = 50) { e =>
      e.processChunks(ctx(1L, "118-HR-1"), Stream.empty)
    }

    val _ = result match {
      case ProcessingResult.Succeeded(naturalKey, _) => naturalKey shouldBe "118-HR-1"
      case other                                     => fail(s"Expected Succeeded but got $other")
    }
    val _ = embedder.batches shouldBe empty
    rawRepo.allRows shouldBe empty
  }

  it should "report a Left when the chunk stream raises mid-submission" in {
    val embedder    = new RecordingEmbeddingService
    val rawRepo     = new RecordingRawRepo
    val raisedError = new IllegalStateException("upstream extractor failed")

    val attempt = runWithEmbedder(embedder, rawRepo, embedBatchSize = 50) { e =>
      val failingStream = Stream.emit("ok") ++ Stream.raiseError[IO](raisedError)
      e.processChunks(ctx(1L, "118-HR-1"), failingStream).attempt
    }

    val _ = attempt.isLeft shouldBe true
    attempt.left.toOption.map(_.getMessage) shouldBe Some("upstream extractor failed")
  }

}
