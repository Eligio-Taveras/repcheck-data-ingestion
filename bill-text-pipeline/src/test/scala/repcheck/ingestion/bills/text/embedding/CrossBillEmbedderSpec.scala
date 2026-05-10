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
 * Each test wraps its IO in `.timeout(TestTimeout)` (currently 30 seconds — bumped from 2s after the cross-subproject
 * test-parallelism flake; see the `TestTimeout` comment below). The timeout is cheap insurance: the FG-only design has
 * no background fibers, so nothing should ever actually hang.
 *
 * What we cover:
 *   - empty-stream ACK: ackId with `expected = Some(0)` → ack fires immediately, no UPSERT, but trim(0) + markFetched
 *     DO run so `bill_text_versions.fetched_at` flips to NOT NULL (otherwise the row sits "incomplete" forever)
 *   - multi-chunk happy path: ack fires after the last chunk persists; trim + markFetched run once
 *   - cross-bill batching: chunks from multiple ackIds in one batch, each ackId completes independently
 *   - concurrent identical (same versionId, two ackIds): both ACK; UPSERT idempotent; both trim + markFetched run
 *   - trim removes stale tail: re-submission with FEWER chunks deletes the leftover tail rows
 *   - embed error → NACK every ackId in the failed batch
 *   - DB UPSERT error → NACK every ackId in the failed batch
 *   - trim error → NACK that ackId; no ack fires
 *   - markFetched error → NACK that ackId; no ack fires
 *   - chunk-stream error in `submit` → NACK and remove ackId from state
 *   - producer fiber cancellation → NACK + cleanup state via `guaranteeCase`
 *   - orphan-buffer cleanup: chunks for a failed ackId are purged before they leak into another ackId's flush
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

    override def upsertMany(rows: List[RawBillTextDO]): ConnectionIO[Int] = {
      // Simulate PostgreSQL's behavior: `INSERT ... ON CONFLICT DO UPDATE` raises
      // "ERROR: ON CONFLICT DO UPDATE command cannot affect row a second time" when the same conflict key
      // appears more than once in a single statement. The embedder is responsible for de-duplicating by
      // (versionId, chunkIndex) before calling upsertMany; this test repo enforces that contract so the
      // unit specs catch any regression that lets duplicates slip through.
      val conflictKeys = rows.flatMap(r => r.versionId.map(v => (v, r.chunkIndex)))
      if (conflictKeys.distinct.size != conflictKeys.size) {
        doobie.free.connection.raiseError(
          new java.sql.SQLException(
            "ON CONFLICT DO UPDATE command cannot affect row a second time (simulated PG dup-key)"
          )
        )
      } else {
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
      }
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

  "submit" should "ACK + run markFetched on an empty chunk stream so the version row reflects 'processed, no text'" in {
    // Empty extraction is a legitimate outcome (e.g., a corrupted source returning zero bytes). Without markFetched
    // here, `bill_text_versions.fetched_at` would stay NULL forever even though the Pub/Sub message ACKed, leaving
    // the row perpetually "incomplete" in the DB. trim past 0 wipes any prior chunks for that versionId
    // (LWW-consistent: latest submission decided "no text").
    val embedder        = new RecordingEmbeddingService
    val rawRepo         = new RecordingRawRepo
    val textVersionRepo = new RecordingTextVersionRepo
    val rec             = ackRecord()
    val billCtx         = ctx(1L, "118-HR-1")

    runWithEmbedder(embedder, rawRepo, textVersionRepo, batchSize = 50) { e =>
      e.submit(billCtx, Stream.empty, "ack-1", rec.ackEffect, rec.nackEffect)
    }

    val _ = rec.acks shouldBe 1
    val _ = rec.nacks shouldBe 0
    val _ = embedder.batches shouldBe empty
    val _ = rawRepo.allRows shouldBe empty
    // markFetched fires exactly once with this versionId — `fetched_at` flips to NOT NULL.
    textVersionRepo.markedVersions shouldBe Vector(billCtx.versionId)
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

  // ===========================================================================
  // Defensive `case None` paths — ackId already removed by a prior failure / cleanup.
  // These exercise the embedder's idempotency guarantees: late callers that find the ackId
  // missing from `state.acks` no-op cleanly without raising.
  // ===========================================================================

  it should "drop chunks in offerChunk when the ackId is not registered" in {
    // Direct exercise of the offerChunk guard (line 176-177): if state.acks doesn't have the ackId
    // (e.g., a prior failBatch removed it while another producer fiber was still streaming), the
    // offered chunk is silently dropped — no buffer growth, no flush, no exception.
    val embedder = new RecordingEmbeddingService
    val rawRepo  = new RecordingRawRepo
    val textRepo = new RecordingTextVersionRepo
    runWithEmbedder(embedder, rawRepo, textRepo, batchSize = 50) { e =>
      // No `register` was called for ack-ghost; calling offerChunk directly should be a no-op.
      e.offerChunk(ctx(1L, "118-HR-1"), 0, "ghost-chunk", "ack-ghost")
    }
    val _ = embedder.batches shouldBe empty
    rawRepo.allRows shouldBe empty
  }

  it should "no-op finalizeSubmission when the ackId was already removed (e.g., by a prior failBatch)" in {
    // Exercises the `case None` branch (lines 211, 213): state.acks lacks the ackId so no flush is
    // triggered and no completion fires.
    val embedder = new RecordingEmbeddingService
    val rawRepo  = new RecordingRawRepo
    val textRepo = new RecordingTextVersionRepo
    runWithEmbedder(embedder, rawRepo, textRepo, batchSize = 50) { e =>
      e.finalizeSubmission("ack-never-registered", count = 5)
    }
    val _ = embedder.batches shouldBe empty
    rawRepo.allRows shouldBe empty
  }

  it should "no-op cleanupOnSubmitError when the ackId is already gone (logger.warn path)" in {
    // Exercises the `case None` branch in cleanupOnSubmitError (lines 460-467): the logger.warn
    // path runs when a submit-time error fires for an ackId that's already been removed (e.g.,
    // a prior failBatch beat us to it). We accept the warn-and-swallow without raising.
    val embedder = new RecordingEmbeddingService
    val rawRepo  = new RecordingRawRepo
    val textRepo = new RecordingTextVersionRepo
    runWithEmbedder(embedder, rawRepo, textRepo, batchSize = 50) { e =>
      e.cleanupOnSubmitError("ack-already-gone", new java.io.IOException("upstream fail"))
    }
    // No exception raised, no chunks persisted.
    val _ = embedder.batches shouldBe empty
    rawRepo.allRows shouldBe empty
  }

  it should "no-op applyBatchSuccess for an ackId removed mid-flight (concurrent failBatch race)" in {
    // Simulates the race where a prior failBatch removed ackId-A from state, but a concurrent
    // applyBatchSuccess for that ackId still arrives — `case None` path (line 307) keeps the
    // accumulator unchanged and continues with siblings.
    val embedder = new RecordingEmbeddingService
    val rawRepo  = new RecordingRawRepo
    val textRepo = new RecordingTextVersionRepo
    val ghostBatch = List(
      ChunkSubmission(ctx(1L, "118-HR-1"), 0, "ghost-text", "ack-removed")
    )
    runWithEmbedder(embedder, rawRepo, textRepo, batchSize = 50)(e => e.applyBatchSuccess(ghostBatch))
    // `applyBatchSuccess` doesn't itself touch the embed service or repo (it only updates state
    // and fires completion); with no ackId in state it's a clean no-op.
    val _ = embedder.batches shouldBe empty
    rawRepo.allRows shouldBe empty
  }

  it should "purge buffered chunks for failing ackIds when failBatch runs with non-empty buffer" in {
    // Exercises the buffer-filter lambda inside failBatch (line 421). Direct-call form so the
    // sequence is deterministic (parTupled-driven concurrency was flaky in CI):
    //   1. register ack-A (so offerChunk doesn't trip the not-in-acks guard)
    //   2. offer a chunk for ack-A — lands in the buffer (size < batchSize)
    //   3. call failBatch directly with a synthetic batch for ack-A — this exercises BOTH
    //      the inner-fold removal (removing A from `acks`) AND the buffer-filter lambda
    //      (iterating over buffer entries to drop A's chunks)
    val embedder = new RecordingEmbeddingService
    val rawRepo  = new RecordingRawRepo
    val textRepo = new RecordingTextVersionRepo
    val rec      = ackRecord()
    val billCtx  = ctx(1L, "118-HR-1")

    runWithEmbedder(embedder, rawRepo, textRepo, batchSize = 50) { e =>
      for {
        _ <- e.register(billCtx, "ack-A", rec.ackEffect, rec.nackEffect)
        // Seed the shared buffer with a chunk for ack-A.
        _ <- e.offerChunk(billCtx, 0, "buffered-text", "ack-A")
        // Now call failBatch with a synthetic batch that attributes the chunk to ack-A; this
        // forces the fold to hit the `Some(progress)` branch (removing A from acks AND nacking)
        // and the buffer-filter lambda to evaluate `ackIdSet.contains(sub.ackId)` on the
        // buffered submission, returning true → that entry gets filtered OUT.
        _ <- e.failBatch(
          List(ChunkSubmission(billCtx, 0, "buffered-text", "ack-A")),
          new java.io.IOException("synthetic batch fail"),
        )
      } yield ()
    }

    // The ackId got NACKed and the buffered chunk was purged (no flush ever happened, so no
    // upsertMany call — but more importantly the lambda inside failBatch's filterNot ran).
    val _ = rec.nacks should be >= 1
    rawRepo.allRows shouldBe empty
  }

  it should "no-op failBatch when none of the batch's ackIds are in state (inner case None at line 418)" in {
    // Exercises the inner `case None` in failBatch's foldLeft (line 418): all ackIds in the
    // failed batch are already gone from state — the fold accumulator stays unchanged and the
    // resulting `removed` list is empty so no nack effects fire.
    val embedder = new RecordingEmbeddingService
    val rawRepo  = new RecordingRawRepo
    val textRepo = new RecordingTextVersionRepo
    val ghostBatch = List(
      ChunkSubmission(ctx(1L, "118-HR-1"), 0, "g0", "ghost-A"),
      ChunkSubmission(ctx(1L, "118-HR-1"), 1, "g1", "ghost-B"),
    )
    runWithEmbedder(embedder, rawRepo, textRepo, batchSize = 50) { e =>
      // Neither ghost-A nor ghost-B was ever registered; failBatch should fold over them
      // hitting the `case None` branch each time and return without any state change or nack.
      e.failBatch(ghostBatch, new java.sql.SQLException("simulated"))
    }
    // No exception, no chunks, no batches embedded.
    val _ = embedder.batches shouldBe empty
    rawRepo.allRows shouldBe empty
  }

}
