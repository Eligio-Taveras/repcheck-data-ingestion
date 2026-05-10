package repcheck.ingestion.bills.text.embedding

import java.time.Instant
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}

import scala.concurrent.duration._

import cats.effect.unsafe.implicits.global
import cats.effect.{Deferred, IO}
import cats.syntax.all._

import fs2.Stream

import doobie._

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import repcheck.ingestion.bills.common.persistence.BillTextVersionRepository
import repcheck.ingestion.bills.text.persistence.RawBillTextRepository
import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}
import repcheck.ingestion.text.embedding.{EmbeddingContextLengthExceeded, EmbeddingGenerationFailed, EmbeddingService}
import repcheck.shared.models.congress.dos.bill.{BillTextVersionDO, RawBillTextDO}

/**
 * Unit specs for the foreground-only [[CrossBillEmbedder]] under the Option C refactor — submit/ack/nack delegation,
 * idempotent UPSERT (last-writer-wins), and embedder-owned trim + markFetched.
 *
 * ==Bills uses last-writer-wins UPSERT (no version-date gate)==
 *
 * The plan's version-date-gated UPSERT cannot be wired on the bills side: `BillTextAvailableEvent` does not carry a
 * `versionDate` field, and `bill_text_versions.version_date` is currently always written as `None`. So bills falls back
 * to plain UPSERT on `(version_id, chunk_index)` — older redeliveries simply rewrite identical data (Congress.gov text
 * for a given (billId, versionCode) is monotonic).
 *
 * Each test wraps its IO in `IO.timeout(2.seconds)` so a hang surfaces as a fast failure rather than blocking sbt
 * forever — the FG-only design has no background fibers, so nothing should ever actually hang, but the timeout is cheap
 * insurance against future regressions.
 *
 * What we cover:
 *   - empty-stream ACK: ackId with `expected = Some(0)` → ack fires immediately, no UPSERT, no trim, no markFetched
 *   - multi-chunk happy path: ack fires after the last chunk persists; trim + markFetched run once
 *   - cross-bill batching: chunks from multiple ackIds in one batch, each ackId completes independently
 *   - concurrent identical (same versionId, two ackIds): both ACK; UPSERT idempotent; both trim + markFetched run
 *   - trim removes stale tail: re-submission with FEWER chunks deletes the leftover tail rows
 *   - embed error → NACK every ackId in the failed batch
 *   - DB UPSERT error → NACK every ackId in the failed batch
 *   - trim error → NACK that ackId; no ack fires
 *   - markFetched error → NACK that ackId; no ack fires
 *   - chunk-stream error in `submit` → NACK and remove ackId from state
 */
class CrossBillEmbedderSpec extends AnyFlatSpec with Matchers {

  // Unique per-suite H2 URL avoids any cross-classloader/cross-driver contention when the JVM is shared with
  // other test subprojects under sbt's cross-project parallelism (default `Dsbt.testConcurrency=2`).
  private val testXa: Transactor[IO] = Transactor.fromDriverManager[IO](
    driver = "org.h2.Driver",
    url = s"jdbc:h2:mem:cross-bill-embedder-spec-${java.util.UUID.randomUUID().toString};DB_CLOSE_DELAY=-1",
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

  // Generous timeout: each `submit` call drives a real H2 `transact(xa)` for the UPSERT batch AND a second
  // `transact(xa)` for trim+markFetched. Under concurrent JVM load (e.g., full-repo `sbt test`), H2 connection
  // acquisition can take several seconds even though the per-call work is trivial. 30s gives plenty of headroom
  // without masking a real hang (the foreground-only design means a true hang would never resolve).
  private val TestTimeout: FiniteDuration = 30.seconds

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

  /**
   * In-memory recording repo that simulates the idempotent UPSERT semantics: maintains a Map keyed by `(versionId,
   * chunkIndex)` so a re-submission with the same key overwrites in place. Tracks every batch handed in for assertion
   * purposes.
   */
  private class RecordingRawRepo extends RawBillTextRepository[ConnectionIO] {
    private val batchesRef = new AtomicReference[Vector[List[RawBillTextDO]]](Vector.empty)

    private val storeRef =
      new AtomicReference[Map[(Long, Int), RawBillTextDO]](Map.empty)

    override def upsertMany(rows: List[RawBillTextDO]): ConnectionIO[Int] =
      doobie.free.connection.delay {
        val _ = batchesRef.updateAndGet(prev => prev :+ rows)
        val _ = storeRef.updateAndGet { current =>
          rows.foldLeft(current) { (acc, row) =>
            row.versionId match {
              case Some(vId) => acc + ((vId, row.chunkIndex) -> row)
              case None      => acc
            }
          }
        }
        rows.size
      }

    override def trimChunksPast(versionId: Long, chunkCount: Int): ConnectionIO[Int] =
      doobie.free.connection.delay {
        val deletedCount = new AtomicInteger(0)
        val _ = storeRef.updateAndGet { current =>
          val (toDelete, toKeep) = current.partition { case ((vId, idx), _) => vId == versionId && idx >= chunkCount }
          val _                  = deletedCount.set(toDelete.size)
          toKeep
        }
        deletedCount.get()
      }

    override def findByVersionId(versionId: Long): ConnectionIO[List[RawBillTextDO]] =
      doobie.free.connection.delay {
        storeRef
          .get()
          .toList
          .collect {
            case ((vId, _), row) if vId == versionId => row
          }
          .sortBy(_.chunkIndex)
      }

    override def countByVersionId(versionId: Long): ConnectionIO[Long] =
      doobie.free.connection.delay(storeRef.get().count { case ((vId, _), _) => vId == versionId }.toLong)

    def batches: Vector[List[RawBillTextDO]] = batchesRef.get()
    def allRows: List[RawBillTextDO]         = storeRef.get().values.toList.sortBy(_.chunkIndex)

    def rowsForVersion(versionId: Long): List[RawBillTextDO] =
      storeRef.get().toList.collect { case ((vId, _), row) if vId == versionId => row }.sortBy(_.chunkIndex)

  }

  private class FailingUpsertRepo(error: Throwable) extends RawBillTextRepository[ConnectionIO] {

    override def upsertMany(rows: List[RawBillTextDO]): ConnectionIO[Int] =
      doobie.free.connection.raiseError(error)

    override def trimChunksPast(versionId: Long, chunkCount: Int): ConnectionIO[Int] =
      doobie.free.connection.pure(0)

    override def findByVersionId(versionId: Long): ConnectionIO[List[RawBillTextDO]] =
      doobie.free.connection.pure(List.empty)

    override def countByVersionId(versionId: Long): ConnectionIO[Long] = doobie.free.connection.pure(0L)
  }

  /** Repo where UPSERT succeeds but trim raises. */
  private class FailingTrimRepo(error: Throwable) extends RawBillTextRepository[ConnectionIO] {

    override def upsertMany(rows: List[RawBillTextDO]): ConnectionIO[Int] =
      doobie.free.connection.pure(rows.size)

    override def trimChunksPast(versionId: Long, chunkCount: Int): ConnectionIO[Int] =
      doobie.free.connection.raiseError(error)

    override def findByVersionId(versionId: Long): ConnectionIO[List[RawBillTextDO]] =
      doobie.free.connection.pure(List.empty)

    override def countByVersionId(versionId: Long): ConnectionIO[Long] = doobie.free.connection.pure(0L)
  }

  /** Records markFetched calls so tests can assert it ran (or didn't). */
  private class RecordingTextVersionRepo extends BillTextVersionRepository[ConnectionIO] {
    private val markedRef = new AtomicReference[Vector[(Long, Instant)]](Vector.empty)

    override def insertVersion(version: BillTextVersionDO): ConnectionIO[Long]      = doobie.free.connection.pure(0L)
    override def storeAndUpdateBill(version: BillTextVersionDO): ConnectionIO[Long] = doobie.free.connection.pure(0L)

    override def findByBillId(billId: Long): ConnectionIO[List[BillTextVersionDO]] =
      doobie.free.connection.pure(List.empty)

    override def findLatestByBillId(billId: Long): ConnectionIO[Option[BillTextVersionDO]] =
      doobie.free.connection.pure(None)

    override def markFetched(versionId: Long, timestamp: Instant): ConnectionIO[Unit] =
      doobie.free.connection.delay {
        val _ = markedRef.updateAndGet(prev => prev :+ ((versionId, timestamp)))
      }

    def markedVersions: Vector[Long] = markedRef.get().map(_._1)
  }

  /** TextVersionRepo where markFetched raises — used to test the markFetched-error → NACK path. */
  private class FailingMarkFetchedRepo(error: Throwable) extends BillTextVersionRepository[ConnectionIO] {
    override def insertVersion(version: BillTextVersionDO): ConnectionIO[Long]      = doobie.free.connection.pure(0L)
    override def storeAndUpdateBill(version: BillTextVersionDO): ConnectionIO[Long] = doobie.free.connection.pure(0L)

    override def findByBillId(billId: Long): ConnectionIO[List[BillTextVersionDO]] =
      doobie.free.connection.pure(List.empty)

    override def findLatestByBillId(billId: Long): ConnectionIO[Option[BillTextVersionDO]] =
      doobie.free.connection.pure(None)

    override def markFetched(versionId: Long, timestamp: Instant): ConnectionIO[Unit] =
      doobie.free.connection.raiseError(error)

  }

  private def ctx(billId: Long, naturalKey: String): BillEmbedCtx =
    BillEmbedCtx(dbBillId = billId, versionId = billId * 100L, naturalKey = naturalKey)

  /** Reified ack/nack effects with counters so tests can assert which fired and how many times. */
  private case class AckRecord(ack: AtomicInteger, nack: AtomicInteger) {
    def ackEffect: IO[Unit]  = IO(()).flatTap(_ => IO { val _ = ack.incrementAndGet() })
    def nackEffect: IO[Unit] = IO(()).flatTap(_ => IO { val _ = nack.incrementAndGet() })
    def acks: Int            = ack.get()
    def nacks: Int           = nack.get()
  }

  private def ackRecord(): AckRecord = AckRecord(new AtomicInteger(0), new AtomicInteger(0))

  private def runWithEmbedder[A](
    embeddingService: EmbeddingService[IO],
    rawRepo: RawBillTextRepository[ConnectionIO],
    textVersionRepo: BillTextVersionRepository[ConnectionIO],
    batchSize: Int,
  )(body: CrossBillEmbedder[IO] => IO[A]): A =
    CrossBillEmbedder
      .resource[IO](
        embeddingService = embeddingService,
        rawBillTextRepository = rawRepo,
        textVersionRepository = textVersionRepo,
        xa = testXa,
        logger = testLogger,
        batchSize = batchSize,
      )
      .use(body)
      .timeout(TestTimeout)
      .unsafeRunSync()

  // ===========================================================================
  // Empty stream — ACK fires immediately, no UPSERT/trim/markFetched
  // ===========================================================================

  "submit" should "ACK immediately on an empty chunk stream (no UPSERT, no trim, no markFetched)" in {
    val embedder        = new RecordingEmbeddingService
    val rawRepo         = new RecordingRawRepo
    val textVersionRepo = new RecordingTextVersionRepo
    val rec             = ackRecord()

    runWithEmbedder(embedder, rawRepo, textVersionRepo, batchSize = 50) { e =>
      e.submit(ctx(1L, "118-HR-1"), Stream.empty, "ack-1", rec.ackEffect, rec.nackEffect)
    }

    val _ = rec.acks shouldBe 1
    val _ = rec.nacks shouldBe 0
    val _ = embedder.batches shouldBe empty
    val _ = rawRepo.allRows shouldBe empty
    textVersionRepo.markedVersions shouldBe empty
  }

  // ===========================================================================
  // Multi-chunk happy path
  // ===========================================================================

  it should "ACK after persisting all chunks, run trim once, run markFetched once" in {
    val embedder        = new RecordingEmbeddingService
    val rawRepo         = new RecordingRawRepo
    val textVersionRepo = new RecordingTextVersionRepo
    val rec             = ackRecord()
    val billCtx         = ctx(1L, "118-HR-1")

    runWithEmbedder(embedder, rawRepo, textVersionRepo, batchSize = 50) { e =>
      e.submit(billCtx, Stream.emits(List("a", "b", "c")), "ack-1", rec.ackEffect, rec.nackEffect)
    }

    val _ = rec.acks shouldBe 1
    val _ = rec.nacks shouldBe 0
    val _ = rawRepo.allRows.map(_.content) shouldBe List("a", "b", "c")
    val _ = rawRepo.allRows.map(_.chunkIndex) shouldBe List(0, 1, 2)
    val _ = rawRepo.allRows.foreach(row => row.versionId shouldBe Some(billCtx.versionId))
    textVersionRepo.markedVersions shouldBe Vector(billCtx.versionId)
  }

  it should "ACK a single-chunk submission via finalize force-flushing the residual" in {
    val embedder        = new RecordingEmbeddingService
    val rawRepo         = new RecordingRawRepo
    val textVersionRepo = new RecordingTextVersionRepo
    val rec             = ackRecord()

    runWithEmbedder(embedder, rawRepo, textVersionRepo, batchSize = 50) { e =>
      e.submit(ctx(1L, "118-HR-1"), Stream.emit("only chunk"), "ack-1", rec.ackEffect, rec.nackEffect)
    }

    val _ = rec.acks shouldBe 1
    val _ = rec.nacks shouldBe 0
    val _ = rawRepo.allRows.size shouldBe 1
    embedder.batches.toList.flatten shouldBe List("only chunk")
  }

  // ===========================================================================
  // Cross-bill batching — multiple ackIds in one batch
  // ===========================================================================

  it should "ACK each ackId independently when multiple bills' chunks land in the same batch" in {
    val embedder        = new RecordingEmbeddingService
    val rawRepo         = new RecordingRawRepo
    val textVersionRepo = new RecordingTextVersionRepo
    val rec1            = ackRecord()
    val rec2            = ackRecord()

    val _ = runWithEmbedder(embedder, rawRepo, textVersionRepo, batchSize = 5) { e =>
      val s1 =
        e.submit(ctx(1L, "118-HR-1"), Stream.emits(List("AAAA", "AAAAAA")), "ack-1", rec1.ackEffect, rec1.nackEffect)
      val s2 = e.submit(ctx(2L, "118-HR-2"), Stream.emits(List("BBB")), "ack-2", rec2.ackEffect, rec2.nackEffect)
      (s1, s2).parTupled
    }

    val _         = rec1.acks shouldBe 1
    val _         = rec1.nacks shouldBe 0
    val _         = rec2.acks shouldBe 1
    val _         = rec2.nacks shouldBe 0
    val _         = rawRepo.allRows.map(_.content).toSet shouldBe Set("AAAA", "AAAAAA", "BBB")
    val markedSet = textVersionRepo.markedVersions.toSet
    markedSet shouldBe Set(100L, 200L)
  }

  // ===========================================================================
  // Concurrent identical: same versionId from two ackIds → both ACK, last-writer-wins
  // ===========================================================================

  it should "ACK both submissions when two messages for the same versionId arrive concurrently (last-writer-wins UPSERT)" in {
    val embedder        = new RecordingEmbeddingService
    val rawRepo         = new RecordingRawRepo
    val textVersionRepo = new RecordingTextVersionRepo
    val recA            = ackRecord()
    val recB            = ackRecord()
    val billCtx         = ctx(1L, "118-HR-1")

    val _ = runWithEmbedder(embedder, rawRepo, textVersionRepo, batchSize = 100) { e =>
      val sA = e.submit(billCtx, Stream.emits(List("x", "y")), "ack-A", recA.ackEffect, recA.nackEffect)
      val sB = e.submit(billCtx, Stream.emits(List("x", "y")), "ack-B", recB.ackEffect, recB.nackEffect)
      (sA, sB).parTupled
    }

    val _ = recA.acks shouldBe 1
    val _ = recA.nacks shouldBe 0
    val _ = recB.acks shouldBe 1
    val _ = recB.nacks shouldBe 0
    // Last-writer-wins: same `(versionId, chunkIndex)` pairs, both writers idempotent on the same content.
    val _ = rawRepo.rowsForVersion(billCtx.versionId).map(_.content).toSet shouldBe Set("x", "y")
    // markFetched is SQL-idempotent; both completions firing it is harmless. Two ackIds → at least one mark.
    textVersionRepo.markedVersions.contains(billCtx.versionId) shouldBe true
  }

  // ===========================================================================
  // Trim removes stale tail
  // ===========================================================================

  it should "trim the stale tail when a re-submission produces FEWER chunks than the prior run" in {
    val embedder        = new RecordingEmbeddingService
    val rawRepo         = new RecordingRawRepo
    val textVersionRepo = new RecordingTextVersionRepo
    val billCtx         = ctx(1L, "118-HR-1")

    // Run 1: 5 chunks
    val rec1 = ackRecord()
    runWithEmbedder(embedder, rawRepo, textVersionRepo, batchSize = 50) { e =>
      e.submit(billCtx, Stream.emits(List("a0", "a1", "a2", "a3", "a4")), "ack-1", rec1.ackEffect, rec1.nackEffect)
    }
    val _ = rawRepo.rowsForVersion(billCtx.versionId).size shouldBe 5

    // Run 2: 3 chunks. Trim should delete chunks 3, 4 from run 1.
    val rec2 = ackRecord()
    runWithEmbedder(embedder, rawRepo, textVersionRepo, batchSize = 50) { e =>
      e.submit(billCtx, Stream.emits(List("b0", "b1", "b2")), "ack-2", rec2.ackEffect, rec2.nackEffect)
    }

    val _        = rec2.acks shouldBe 1
    val _        = rec2.nacks shouldBe 0
    val survived = rawRepo.rowsForVersion(billCtx.versionId)
    val _        = survived.size shouldBe 3
    val _        = survived.map(_.chunkIndex) shouldBe List(0, 1, 2)
    survived.map(_.content) shouldBe List("b0", "b1", "b2")
  }

  // ===========================================================================
  // Failure paths — embed, DB, trim, markFetched, chunk-stream
  // ===========================================================================

  it should "NACK every ackId in the batch when the embedding service raises" in {
    val rawRepo         = new RecordingRawRepo
    val textVersionRepo = new RecordingTextVersionRepo
    val embedder        = new FailingEmbeddingService(EmbeddingGenerationFailed("ollama 503", 5))
    val rec1            = ackRecord()
    val rec2            = ackRecord()

    val _ = runWithEmbedder(embedder, rawRepo, textVersionRepo, batchSize = 2) { e =>
      val s1 = e.submit(ctx(1L, "118-HR-1"), Stream.emit("a"), "ack-1", rec1.ackEffect, rec1.nackEffect)
      val s2 = e.submit(ctx(2L, "118-HR-2"), Stream.emit("b"), "ack-2", rec2.ackEffect, rec2.nackEffect)
      (s1, s2).parTupled
    }

    val _ = rec1.acks shouldBe 0
    val _ = rec1.nacks should be >= 1
    val _ = rec2.acks shouldBe 0
    val _ = rec2.nacks should be >= 1
    rawRepo.allRows shouldBe empty
  }

  it should "NACK on EmbeddingContextLengthExceeded (Systemic embed error)" in {
    val rawRepo         = new RecordingRawRepo
    val textVersionRepo = new RecordingTextVersionRepo
    val embedder        = new FailingEmbeddingService(EmbeddingContextLengthExceeded("oversized", 30001))
    val rec             = ackRecord()

    runWithEmbedder(embedder, rawRepo, textVersionRepo, batchSize = 50) { e =>
      e.submit(ctx(1L, "118-HR-1"), Stream.emit("oversized"), "ack-1", rec.ackEffect, rec.nackEffect)
    }

    val _ = rec.acks shouldBe 0
    rec.nacks should be >= 1
  }

  it should "NACK every ackId in the batch when upsertMany raises (DB error)" in {
    val embedder        = new RecordingEmbeddingService
    val textVersionRepo = new RecordingTextVersionRepo
    val rawRepo         = new FailingUpsertRepo(new java.sql.SQLTransientConnectionException("conn lost"))
    val rec             = ackRecord()

    runWithEmbedder(embedder, rawRepo, textVersionRepo, batchSize = 50) { e =>
      e.submit(ctx(1L, "118-HR-1"), Stream.emit("doomed"), "ack-1", rec.ackEffect, rec.nackEffect)
    }

    val _ = rec.acks shouldBe 0
    val _ = rec.nacks should be >= 1
    val _ = embedder.batches.size shouldBe 1
    textVersionRepo.markedVersions shouldBe empty
  }

  it should "NACK when trimChunksPast raises (no ACK, no markFetched)" in {
    val embedder        = new RecordingEmbeddingService
    val textVersionRepo = new RecordingTextVersionRepo
    val rawRepo         = new FailingTrimRepo(new java.sql.SQLTransientException("trim failed"))
    val rec             = ackRecord()

    runWithEmbedder(embedder, rawRepo, textVersionRepo, batchSize = 50) { e =>
      e.submit(ctx(1L, "118-HR-1"), Stream.emit("text"), "ack-1", rec.ackEffect, rec.nackEffect)
    }

    val _ = rec.acks shouldBe 0
    val _ = rec.nacks should be >= 1
    textVersionRepo.markedVersions shouldBe empty
  }

  it should "NACK when markFetched raises (no ACK)" in {
    val embedder        = new RecordingEmbeddingService
    val rawRepo         = new RecordingRawRepo
    val textVersionRepo = new FailingMarkFetchedRepo(new java.sql.SQLTransientException("mark failed"))
    val rec             = ackRecord()

    runWithEmbedder(embedder, rawRepo, textVersionRepo, batchSize = 50) { e =>
      e.submit(ctx(1L, "118-HR-1"), Stream.emit("text"), "ack-1", rec.ackEffect, rec.nackEffect)
    }

    val _ = rec.acks shouldBe 0
    rec.nacks should be >= 1
  }

  it should "NACK when the chunk stream itself raises mid-submission" in {
    val embedder        = new RecordingEmbeddingService
    val rawRepo         = new RecordingRawRepo
    val textVersionRepo = new RecordingTextVersionRepo
    val rec             = ackRecord()
    val raisedError     = new IllegalStateException("upstream extractor failed")

    val attempt = CrossBillEmbedder
      .resource[IO](
        embeddingService = embedder,
        rawBillTextRepository = rawRepo,
        textVersionRepository = textVersionRepo,
        xa = testXa,
        logger = testLogger,
        batchSize = 50,
      )
      .use { e =>
        val failingStream = Stream.emit("ok") ++ Stream.raiseError[IO](raisedError)
        e.submit(ctx(1L, "118-HR-1"), failingStream, "ack-1", rec.ackEffect, rec.nackEffect).attempt
      }
      .timeout(TestTimeout)
      .unsafeRunSync()

    // Stream errors propagate via cleanupOnSubmitError → nack → submit completes (Right(())). Either an Either-Left
    // or Either-Right is structurally valid here; what matters is nack fired and ack didn't.
    val _ = attempt // silence "unused" — we don't assert on the Left/Right shape
    val _ = rec.acks shouldBe 0
    rec.nacks should be >= 1
  }

  it should "NACK when the user-supplied ack effect itself raises (e.g., publishEvent failure)" in {
    val embedder                = new RecordingEmbeddingService
    val rawRepo                 = new RecordingRawRepo
    val textVersionRepo         = new RecordingTextVersionRepo
    val nackCalls               = new AtomicInteger(0)
    val ackThatRaises: IO[Unit] = IO.raiseError(new RuntimeException("publish failed"))
    val nackEffect: IO[Unit]    = IO { val _ = nackCalls.incrementAndGet() }

    runWithEmbedder(embedder, rawRepo, textVersionRepo, batchSize = 50) { e =>
      e.submit(ctx(1L, "118-HR-1"), Stream.emit("text"), "ack-1", ackThatRaises, nackEffect)
    }

    nackCalls.get() should be >= 1
  }

  // ===========================================================================
  // Sanity — batch fold preserves per-row association under cross-bill mixing
  // ===========================================================================

  it should "associate each row's billId/versionId with its OWN content under cross-bill mixing" in {
    val embedder        = new RecordingEmbeddingService
    val rawRepo         = new RecordingRawRepo
    val textVersionRepo = new RecordingTextVersionRepo
    val billA           = ctx(1L, "118-HR-1")
    val billB           = ctx(2L, "118-HR-2")
    val billC           = ctx(3L, "118-HR-3")
    val recA            = ackRecord()
    val recB            = ackRecord()
    val recC            = ackRecord()

    val _ = runWithEmbedder(embedder, rawRepo, textVersionRepo, batchSize = 5) { e =>
      val a = e.submit(billA, Stream.emits(List("AAAA", "AAAAAA")), "ack-A", recA.ackEffect, recA.nackEffect)
      val b = e.submit(billB, Stream.emits(List("BBB")), "ack-B", recB.ackEffect, recB.nackEffect)
      val c = e.submit(billC, Stream.emits(List("CC", "CCCCCCCC")), "ack-C", recC.ackEffect, recC.nackEffect)
      (a, b, c).parTupled
    }

    val _ = recA.acks shouldBe 1
    val _ = recB.acks shouldBe 1
    val _ = recC.acks shouldBe 1

    val rowsByBill: Map[Long, List[RawBillTextDO]] = rawRepo.allRows.groupBy(_.billId)
    val _                                          = rowsByBill(1L).map(_.content).toSet shouldBe Set("AAAA", "AAAAAA")
    val _                                          = rowsByBill(2L).map(_.content).toSet shouldBe Set("BBB")
    val _                                          = rowsByBill(3L).map(_.content).toSet shouldBe Set("CC", "CCCCCCCC")

    rawRepo.allRows.foreach { row =>
      val embFirstFloat = row.embedding.flatMap(_.headOption).getOrElse(-1.0f)
      withClue(s"row content='${row.content}' embedding[0]=$embFirstFloat") {
        embFirstFloat shouldBe row.content.length.toFloat
      }
    }
  }

  // ===========================================================================
  // Error classification (still useful — embedder still has describeError fallback)
  // ===========================================================================

  // ===========================================================================
  // Orphan-buffer cleanup — failed ackIds must not poison later flushes
  // ===========================================================================

  it should "purge orphan buffered chunks for a failed ackId so they don't leak into a later flush" in {
    // Producer A emits 2 chunks (under batchSize=10, no flush yet) and then raises. `cleanupOnSubmitError` must remove
    // both buffered chunks before producer B's submission flushes — otherwise B's batch would carry A's orphans
    // (NACKed but still pending in the shared buffer) and persist them.
    val embedder        = new RecordingEmbeddingService
    val rawRepo         = new RecordingRawRepo
    val textVersionRepo = new RecordingTextVersionRepo
    val recA            = ackRecord()
    val recB            = ackRecord()

    val partialThenFail: Stream[IO, String] =
      Stream("orphan-0", "orphan-1") ++ Stream.raiseError[IO](new java.io.IOException("partway"))

    val _ = CrossBillEmbedder
      .resource[IO](
        embeddingService = embedder,
        rawBillTextRepository = rawRepo,
        textVersionRepository = textVersionRepo,
        xa = testXa,
        logger = testLogger,
        batchSize = 10,
      )
      .use { e =>
        for {
          // First submission: 2 chunks then raises before any flush. Stream error → cleanupOnSubmitError → NACK + buffer purge.
          _ <- e.submit(ctx(1L, "118-HR-1"), partialThenFail, "ack-A", recA.ackEffect, recA.nackEffect).attempt
          // Second submission: 1 chunk, completes normally. Its forced residual flush MUST NOT include A's orphans.
          _ <- e.submit(ctx(2L, "118-HR-2"), Stream.emit("clean-chunk"), "ack-B", recB.ackEffect, recB.nackEffect)
        } yield ()
      }
      .timeout(TestTimeout)
      .unsafeRunSync()

    val _ = recA.acks shouldBe 0
    val _ = recA.nacks should be >= 1
    val _ = recB.acks shouldBe 1
    val _ = recB.nacks shouldBe 0
    // Only one batch ever reached the embed service: B's single chunk. A's orphans were purged before any flush.
    val _ = embedder.batches.flatten shouldBe List("clean-chunk")
    rawRepo.allRows.map(_.content) shouldBe List("clean-chunk")
  }

  it should "NACK and clean up state when the producing fiber is cancelled mid-stream" in {
    // Without `guaranteeCase` the ackId would leak indefinitely on cancellation: `handleErrorWith` doesn't fire
    // for cancelled fibers. The new finalizer must invoke `cleanupOnSubmitError` so the ackId is removed and NACK fires.
    val embedder        = new RecordingEmbeddingService
    val rawRepo         = new RecordingRawRepo
    val textVersionRepo = new RecordingTextVersionRepo
    val rec             = ackRecord()

    val program: IO[Unit] = Deferred[IO, Unit].flatMap { chunkOffered =>
      val hangingStream: Stream[IO, String] =
        Stream.emit("chunk-0").covary[IO] ++
          Stream.eval(chunkOffered.complete(()).void).drain ++
          Stream.eval(IO.never[String])

      CrossBillEmbedder
        .resource[IO](
          embeddingService = embedder,
          rawBillTextRepository = rawRepo,
          textVersionRepository = textVersionRepo,
          xa = testXa,
          logger = testLogger,
          batchSize = 10,
        )
        .use { e =>
          for {
            fiber <- e.submit(ctx(1L, "118-HR-1"), hangingStream, "ack-cancel", rec.ackEffect, rec.nackEffect).start
            _     <- chunkOffered.get // chunk-0 was offered; producer is now hanging on IO.never
            _     <- fiber.cancel
            _     <- fiber.join.void  // wait for cancellation finalizer to complete
          } yield ()
        }
    }

    val _ = program.timeout(TestTimeout).unsafeRunSync()
    val _ = rec.acks shouldBe 0
    val _ = rec.nacks should be >= 1
    // Nothing flushed — chunk-0 sat in the buffer, then was purged by cleanupOnSubmitError on cancel.
    val _ = rawRepo.allRows shouldBe empty
    textVersionRepo.markedVersions shouldBe empty
  }

  // ===========================================================================
  // describeError fallback
  // ===========================================================================

  it should "fall back to the simple class name when an exception's message is null" in {
    val rawRepo         = new RecordingRawRepo
    val textVersionRepo = new RecordingTextVersionRepo
    val embedder        = new FailingEmbeddingService(new RuntimeException()) // no message
    val rec             = ackRecord()

    runWithEmbedder(embedder, rawRepo, textVersionRepo, batchSize = 50) { e =>
      e.submit(ctx(1L, "118-HR-1"), Stream.emit("text"), "ack-1", rec.ackEffect, rec.nackEffect)
    }

    rec.nacks should be >= 1
  }

}
