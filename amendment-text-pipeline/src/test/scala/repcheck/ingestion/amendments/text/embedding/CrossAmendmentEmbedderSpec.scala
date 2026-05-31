package repcheck.ingestion.amendments.text.embedding

import java.time.Instant
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}

import scala.concurrent.duration._

import cats.effect.unsafe.implicits.global
import cats.effect.{Deferred, IO}

import fs2.Stream

import doobie._

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import repcheck.ingestion.amendments.text.persistence.{
  AmendmentChunkRow,
  AmendmentTextChunkRepository,
  AmendmentTextVersionRepository,
}
import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}
import repcheck.ingestion.text.embedding.{EmbeddingContextLengthExceeded, EmbeddingGenerationFailed, EmbeddingService}
import repcheck.shared.models.congress.dos.amendment.{AmendmentTextChunkDO, AmendmentTextVersionDO}

/**
 * Unit specs for the foreground-only [[CrossAmendmentEmbedder]].
 *
 * ==Last-writer-wins UPSERT — no version-date gate==
 *
 * Amendments use plain `INSERT ... ON CONFLICT (version_id, chunk_index) DO UPDATE` rather than the version-date-gated
 * CTE on the bills side. The natural identity for a chunk is `(version_id, chunk_index)`; same identity → same
 * Congress.gov bytes → same chunks (chunker is deterministic), so the UPDATE branch overwrites with effectively the
 * same data on the happy path. The only scenario the gate would protect against — Congress.gov mutating published
 * amendment text for a given `(amendment_id, version_type, format_type)` — doesn't happen in practice; recovery for the
 * hypothetical case is "re-emit the event" and the UPSERT overwrites.
 *
 * Consequences for this spec: no "older redelivery filtered" or "newer beats older mid-flight" tests — those required
 * the gate. The remaining test surface covers: empty stream, multi-chunk happy path, concurrent identical redelivery
 * (both ACK), trim-removes-stale-tail, and the four NACK error paths (embed, DB, trim, markFetched).
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

  /**
   * In-memory chunk repo. `upsertMany` returns the count of rows written (echoing input size on the happy path),
   * `trimChunksPast` returns 0 by default. Configurable failure injection for the trim/upsert error cases.
   */
  private class RecordingChunkRepo(
    upsertFailure: Option[Throwable] = None,
    trimFailure: Option[Throwable] = None,
  ) extends AmendmentTextChunkRepository[ConnectionIO] {
    private val rowsRef             = new AtomicReference[Vector[List[AmendmentChunkRow]]](Vector.empty)
    private val trimCallsRef        = new AtomicReference[Vector[(Long, Int)]](Vector.empty)
    private val sumContentLengthRef = new AtomicReference[Long](0L)

    override def upsertMany(rows: List[AmendmentChunkRow]): ConnectionIO[Int] =
      upsertFailure match {
        case Some(err) => doobie.free.connection.raiseError(err)
        case None      =>
          // Simulate PostgreSQL's behavior: `INSERT ... ON CONFLICT DO UPDATE` raises
          // "ERROR: ON CONFLICT DO UPDATE command cannot affect row a second time" when the same conflict key
          // appears more than once in a single statement. The embedder is responsible for de-duplicating by
          // (versionId, chunkIndex) before calling upsertMany; this test repo enforces that contract so the
          // unit specs catch any regression that lets duplicates slip through.
          val conflictKeys = rows.map(r => (r.versionId, r.chunkIndex))
          if (conflictKeys.distinct.size != conflictKeys.size) {
            doobie.free.connection.raiseError(
              new java.sql.SQLException(
                "ON CONFLICT DO UPDATE command cannot affect row a second time (simulated PG dup-key)"
              )
            )
          } else {
            doobie.free.connection.delay {
              val _ = rowsRef.updateAndGet(prev => prev :+ rows)
              val _ = sumContentLengthRef.updateAndGet(prev => prev + rows.map(_.content.length.toLong).sum)
              rows.size
            }
          }
      }

    override def trimChunksPast(versionId: Long, chunkCount: Int): ConnectionIO[Int] =
      trimFailure match {
        case Some(err) => doobie.free.connection.raiseError(err)
        case None =>
          doobie.free.connection.delay {
            val _ = trimCallsRef.updateAndGet(prev => prev :+ ((versionId, chunkCount)))
            0
          }
      }

    override def countByVersionId(versionId: Long): ConnectionIO[Long] =
      doobie.free.connection.pure(rowsRef.get().flatten.count(_.versionId == versionId).toLong)

    override def sumContentLengthByVersionId(versionId: Long): ConnectionIO[Long] =
      doobie.free.connection.pure(
        rowsRef.get().flatten.filter(_.versionId == versionId).map(_.content.length.toLong).sum
      )

    override def findByVersionId(versionId: Long): ConnectionIO[List[AmendmentTextChunkDO]] =
      doobie.free.connection.pure(List.empty)

    def rows: Vector[List[AmendmentChunkRow]] = rowsRef.get()
    def trimCalls: Vector[(Long, Int)]        = trimCallsRef.get()
  }

  /**
   * In-memory version repo. Captures `markFetched` calls; configurable failure injection for the markFetched error
   * case. `upsert` is unused by the embedder (only the processor uses it) so we leave it as a no-op stub.
   */
  private class RecordingVersionRepo(markFetchedFailure: Option[Throwable] = None)
      extends AmendmentTextVersionRepository[ConnectionIO] {
    private val markFetchedCalls = new AtomicReference[Vector[(Long, Instant, Int)]](Vector.empty)
    private val linkLatestCalls  = new AtomicReference[Vector[(Long, Long)]](Vector.empty)

    override def upsert(version: AmendmentTextVersionDO): ConnectionIO[(Long, Boolean, Boolean)] =
      doobie.free.connection.pure((0L, true, false))

    override def markFetched(versionId: Long, timestamp: Instant, textLength: Int): ConnectionIO[Unit] =
      markFetchedFailure match {
        case Some(err) => doobie.free.connection.raiseError(err)
        case None =>
          doobie.free.connection.delay {
            val _ = markFetchedCalls.updateAndGet(prev => prev :+ ((versionId, timestamp, textLength)))
            ()
          }
      }

    override def linkLatestTextVersion(amendmentId: Long, versionId: Long): ConnectionIO[Unit] =
      doobie.free.connection.delay {
        val _ = linkLatestCalls.updateAndGet(prev => prev :+ ((amendmentId, versionId)))
        ()
      }

    override def findCompletedByAmendmentId(amendmentId: Long): ConnectionIO[List[AmendmentTextVersionDO]] =
      doobie.free.connection.pure(List.empty)

    def markFetchedSeen: Vector[(Long, Instant, Int)] = markFetchedCalls.get()
    def linkLatestSeen: Vector[(Long, Long)]          = linkLatestCalls.get()
  }

  private val testCtx  = AmendmentEmbedCtx(amendmentId = 42L, versionId = 7L, naturalKey = "117-SAMDT-2137")
  private val testCtx2 = AmendmentEmbedCtx(amendmentId = 99L, versionId = 8L, naturalKey = "117-HAMDT-3")

  /**
   * Test-side ACK / NACK counters. Each invocation increments by 1; tests assert exactly one of ACK or NACK fires per
   * ackId.
   */
  final private class AckCounters {
    val acks           = new AtomicInteger(0)
    val nacks          = new AtomicInteger(0)
    val ack: IO[Unit]  = IO { val _ = acks.incrementAndGet(); () }
    val nack: IO[Unit] = IO { val _ = nacks.incrementAndGet(); () }
  }

  "submit" should "ACK + trim(0) + markFetched(text_length=0) on an empty chunk stream so the version row reflects 'processed, no text'" in {
    // Empty extraction is a legitimate outcome (e.g., a corrupted source returning zero bytes). Without trim+markFetched
    // here, `amendment_text_versions.fetched_at` would stay NULL forever even though the Pub/Sub message ACKed,
    // leaving the row perpetually "incomplete" in the DB.
    val embedSvc    = new RecordingEmbeddingService
    val chunkRepo   = new RecordingChunkRepo
    val versionRepo = new RecordingVersionRepo
    val counters    = new AckCounters
    val program = CrossAmendmentEmbedder
      .resource[IO](embedSvc, chunkRepo, versionRepo, testXa, testLogger, batchSize = 50)
      .use(embedder => embedder.submit(testCtx, Stream.empty, "ack-1", counters.ack, counters.nack))
      .timeout(TestTimeout)

    val _ = program.unsafeRunSync()
    val _ = counters.acks.get() shouldBe 1
    val _ = counters.nacks.get() shouldBe 0
    val _ = embedSvc.batches shouldBe empty
    val _ = chunkRepo.rows shouldBe empty
    // Trim past 0 wipes any prior chunks for that versionId (LWW-consistent: latest submission decided "no text").
    val _ = chunkRepo.trimCalls shouldBe Vector((7L, 0))
    // markFetched runs with text_length = 0 — sumContentLengthByVersionId returns 0 because no rows persisted.
    val _ = versionRepo.markFetchedSeen.size shouldBe 1
    val _ = versionRepo.markFetchedSeen.headOption.map(_._1) shouldBe Some(7L)
    versionRepo.markFetchedSeen.headOption.map(_._3) shouldBe Some(0)
  }

  it should "ACK after multi-chunk happy path; trim past tail + markFetched fire when written > 0" in {
    val embedSvc    = new RecordingEmbeddingService
    val chunkRepo   = new RecordingChunkRepo
    val versionRepo = new RecordingVersionRepo
    val counters    = new AckCounters

    val _ = CrossAmendmentEmbedder
      .resource[IO](embedSvc, chunkRepo, versionRepo, testXa, testLogger, batchSize = 50)
      .use(embedder =>
        embedder.submit(testCtx, Stream("chunk-0", "chunk-1", "chunk-2"), "ack-1", counters.ack, counters.nack)
      )
      .timeout(TestTimeout)
      .unsafeRunSync()

    val _ = counters.acks.get() shouldBe 1
    val _ = counters.nacks.get() shouldBe 0
    val _ = embedSvc.batches.size shouldBe 1
    val _ = chunkRepo.rows.flatten.size shouldBe 3
    val _ = chunkRepo.trimCalls shouldBe Vector((7L, 3))
    val _ = versionRepo.markFetchedSeen.size shouldBe 1
    val _ = versionRepo.markFetchedSeen.headOption.map(_._1) shouldBe Some(7L)
    // Completion back-links amendments.latest_text_version_id (amendmentId 42, versionId 7 from testCtx).
    versionRepo.linkLatestSeen shouldBe Vector((42L, 7L))
  }

  it should "preserve chunk_index, amendmentId, and versionId on persisted rows" in {
    val embedSvc    = new RecordingEmbeddingService
    val chunkRepo   = new RecordingChunkRepo
    val versionRepo = new RecordingVersionRepo
    val counters    = new AckCounters
    val _ = CrossAmendmentEmbedder
      .resource[IO](embedSvc, chunkRepo, versionRepo, testXa, testLogger, batchSize = 50)
      .use(embedder => embedder.submit(testCtx, Stream("a", "b"), "ack-1", counters.ack, counters.nack))
      .timeout(TestTimeout)
      .unsafeRunSync()

    val rows = chunkRepo.rows.flatten
    val _    = rows.size shouldBe 2
    val _    = rows.map(_.amendmentId).distinct shouldBe List(42L)
    val _    = rows.map(_.versionId).distinct shouldBe List(7L)
    rows.map(_.chunkIndex).sorted shouldBe List(0, 1)
  }

  it should "NACK when the embedding service raises a network error (DB-error-NACK shape too)" in {
    val embedSvc    = new FailingEmbeddingService(new java.net.SocketTimeoutException("network timeout"))
    val chunkRepo   = new RecordingChunkRepo
    val versionRepo = new RecordingVersionRepo
    val counters    = new AckCounters
    val _ = CrossAmendmentEmbedder
      .resource[IO](embedSvc, chunkRepo, versionRepo, testXa, testLogger, batchSize = 50)
      .use(embedder => embedder.submit(testCtx, Stream.emit("chunk"), "ack-1", counters.ack, counters.nack))
      .timeout(TestTimeout)
      .unsafeRunSync()

    val _ = counters.acks.get() shouldBe 0
    val _ = counters.nacks.get() shouldBe 1
    val _ = chunkRepo.trimCalls shouldBe empty
    versionRepo.markFetchedSeen shouldBe empty
  }

  it should "NACK on EmbeddingContextLengthExceeded" in {
    val embedSvc    = new FailingEmbeddingService(EmbeddingContextLengthExceeded("too big", 100000))
    val chunkRepo   = new RecordingChunkRepo
    val versionRepo = new RecordingVersionRepo
    val counters    = new AckCounters
    val _ = CrossAmendmentEmbedder
      .resource[IO](embedSvc, chunkRepo, versionRepo, testXa, testLogger, batchSize = 50)
      .use(embedder => embedder.submit(testCtx, Stream.emit("oversized"), "ack-1", counters.ack, counters.nack))
      .timeout(TestTimeout)
      .unsafeRunSync()

    val _ = counters.acks.get() shouldBe 0
    counters.nacks.get() shouldBe 1
  }

  it should "NACK on EmbeddingGenerationFailed" in {
    val embedSvc    = new FailingEmbeddingService(EmbeddingGenerationFailed("ollama hiccup", 100))
    val chunkRepo   = new RecordingChunkRepo
    val versionRepo = new RecordingVersionRepo
    val counters    = new AckCounters
    val _ = CrossAmendmentEmbedder
      .resource[IO](embedSvc, chunkRepo, versionRepo, testXa, testLogger, batchSize = 50)
      .use(embedder => embedder.submit(testCtx, Stream.emit("c"), "ack-1", counters.ack, counters.nack))
      .timeout(TestTimeout)
      .unsafeRunSync()

    val _ = counters.acks.get() shouldBe 0
    counters.nacks.get() shouldBe 1
  }

  it should "NACK on DB upsert error" in {
    val embedSvc    = new RecordingEmbeddingService
    val chunkRepo   = new RecordingChunkRepo(upsertFailure = Some(new java.sql.SQLException("constraint violation")))
    val versionRepo = new RecordingVersionRepo
    val counters    = new AckCounters
    val _ = CrossAmendmentEmbedder
      .resource[IO](embedSvc, chunkRepo, versionRepo, testXa, testLogger, batchSize = 50)
      .use(embedder => embedder.submit(testCtx, Stream.emit("chunk"), "ack-1", counters.ack, counters.nack))
      .timeout(TestTimeout)
      .unsafeRunSync()

    val _ = counters.acks.get() shouldBe 0
    counters.nacks.get() shouldBe 1
  }

  it should "NACK on trim error after a successful UPSERT batch" in {
    val embedSvc    = new RecordingEmbeddingService
    val chunkRepo   = new RecordingChunkRepo(trimFailure = Some(new java.sql.SQLException("trim failed")))
    val versionRepo = new RecordingVersionRepo
    val counters    = new AckCounters
    val _ = CrossAmendmentEmbedder
      .resource[IO](embedSvc, chunkRepo, versionRepo, testXa, testLogger, batchSize = 50)
      .use(embedder => embedder.submit(testCtx, Stream.emit("chunk"), "ack-1", counters.ack, counters.nack))
      .timeout(TestTimeout)
      .unsafeRunSync()

    val _ = counters.acks.get() shouldBe 0
    val _ = counters.nacks.get() shouldBe 1
    // markFetched does not fire because trim raised first inside the same transaction.
    versionRepo.markFetchedSeen shouldBe empty
  }

  it should "NACK on markFetched error after a successful trim" in {
    val embedSvc  = new RecordingEmbeddingService
    val chunkRepo = new RecordingChunkRepo
    val versionRepo =
      new RecordingVersionRepo(markFetchedFailure = Some(new java.sql.SQLException("markFetched failed")))
    val counters = new AckCounters
    val _ = CrossAmendmentEmbedder
      .resource[IO](embedSvc, chunkRepo, versionRepo, testXa, testLogger, batchSize = 50)
      .use(embedder => embedder.submit(testCtx, Stream.emit("chunk"), "ack-1", counters.ack, counters.nack))
      .timeout(TestTimeout)
      .unsafeRunSync()

    val _ = counters.acks.get() shouldBe 0
    counters.nacks.get() shouldBe 1
  }

  it should "fall back to NACK when the user-supplied ack effect itself raises (e.g., publishIngestedEvent failure)" in {
    // The processor wires `publishIngestedEvent *> subscriber.acknowledge(ackId)` as the ack effect. If publish raises
    // (transient broker failure), the ackId is already removed from state; if the embedder didn't fall back to NACK
    // here, the Pub/Sub message would just sit until ackDeadline and rely on implicit redelivery. Explicit NACK forces
    // immediate redelivery via modifyAckDeadline=0.
    val embedSvc                = new RecordingEmbeddingService
    val chunkRepo               = new RecordingChunkRepo
    val versionRepo             = new RecordingVersionRepo
    val nackCounter             = new AtomicInteger(0)
    val ackThatRaises: IO[Unit] = IO.raiseError(new RuntimeException("publish failed"))
    val nackEffect: IO[Unit]    = IO { val _ = nackCounter.incrementAndGet(); () }

    val _ = CrossAmendmentEmbedder
      .resource[IO](embedSvc, chunkRepo, versionRepo, testXa, testLogger, batchSize = 50)
      .use(embedder => embedder.submit(testCtx, Stream.emit("chunk"), "ack-1", ackThatRaises, nackEffect))
      .timeout(TestTimeout)
      .unsafeRunSync()

    nackCounter.get() shouldBe 1
  }

  it should "isolate ACK failures so one ackId's raising ack doesn't strand sibling ackIds in state" in {
    // applyBatchResult runs `traverse_(completeAckIfReady)` over every ackId in the batch. If one of those ackIds'
    // ack effect raises and isn't isolated, the traversal short-circuits and later ackIds never get their completion
    // call. With safeAck wrapping ack in `.attempt`, the failed ackId falls back to NACK and the loop continues.
    val embedSvc              = new RecordingEmbeddingService
    val chunkRepo             = new RecordingChunkRepo
    val versionRepo           = new RecordingVersionRepo
    val raisingAck: IO[Unit]  = IO.raiseError(new RuntimeException("publish broken"))
    val failingNackCounter    = new AtomicInteger(0)
    val failingNack: IO[Unit] = IO { val _ = failingNackCounter.incrementAndGet(); () }
    val sibling               = new AckCounters

    // batchSize=2 so the two submissions land in one shared batch
    val _ = CrossAmendmentEmbedder
      .resource[IO](embedSvc, chunkRepo, versionRepo, testXa, testLogger, batchSize = 2)
      .use { embedder =>
        for {
          fiberA <- embedder.submit(testCtx, Stream.emit("a"), "ack-fail", raisingAck, failingNack).start
          fiberB <- embedder.submit(testCtx2, Stream.emit("b"), "ack-ok", sibling.ack, sibling.nack).start
          _      <- fiberA.joinWithNever
          _      <- fiberB.joinWithNever
        } yield ()
      }
      .timeout(TestTimeout)
      .unsafeRunSync()

    // The failing-ack ackId fell back to NACK; its sibling completed normally with ACK.
    val _ = failingNackCounter.get() shouldBe 1
    val _ = sibling.acks.get() shouldBe 1
    sibling.nacks.get() shouldBe 0
  }

  it should "ACK both ackIds when concurrent identical redelivery hits the same versionId (last-writer-wins UPSERT)" in {
    // Two concurrent submit calls for the same (amendmentId, versionId) — Pub/Sub at-least-once redelivery. Without
    // the per-versionId Deferred-join logic of the old design, both submissions just write through the LWW UPSERT;
    // the second's UPDATE branch overwrites with effectively the same data. Both ACKs must fire — neither hangs.
    val embedSvc    = new RecordingEmbeddingService
    val chunkRepo   = new RecordingChunkRepo
    val versionRepo = new RecordingVersionRepo
    val counters1   = new AckCounters
    val counters2   = new AckCounters

    val _ = CrossAmendmentEmbedder
      .resource[IO](embedSvc, chunkRepo, versionRepo, testXa, testLogger, batchSize = 50)
      .use { embedder =>
        for {
          fiber1 <- embedder.submit(testCtx, Stream.emit("dup-chunk"), "ack-A", counters1.ack, counters1.nack).start
          fiber2 <- embedder.submit(testCtx, Stream.emit("dup-chunk"), "ack-B", counters2.ack, counters2.nack).start
          _      <- fiber1.joinWithNever
          _      <- fiber2.joinWithNever
        } yield ()
      }
      .timeout(TestTimeout)
      .unsafeRunSync()

    val _ = counters1.acks.get() shouldBe 1
    val _ = counters1.nacks.get() shouldBe 0
    val _ = counters2.acks.get() shouldBe 1
    val _ = counters2.nacks.get() shouldBe 0
    // Persisted-row count is timing-dependent and BOTH outcomes are correct: if the two fibers' chunks land in one
    // shared flush, the LWW dedup on (versionId, chunkIndex) (both are version 7 / index 0) collapses them to a single
    // row; if they flush separately, two rows are recorded. Attribution uses the full batch either way, so each ackId
    // still completes — markFetched + trim fire twice regardless of which flush interleaving won the race.
    val _ = chunkRepo.rows.flatten.size should (be(1) or be(2))
    val _ = versionRepo.markFetchedSeen.size shouldBe 2
    chunkRepo.trimCalls shouldBe Vector((7L, 1), (7L, 1))
  }

  it should "complete two amendments via cross-amendment batching when batchSize triggers a shared flush" in {
    val embedSvc    = new RecordingEmbeddingService
    val chunkRepo   = new RecordingChunkRepo
    val versionRepo = new RecordingVersionRepo
    val counters1   = new AckCounters
    val counters2   = new AckCounters
    // batchSize=2 → first amendment's 1 chunk fills slot 1, second amendment's 1 chunk triggers the flush.
    val _ = CrossAmendmentEmbedder
      .resource[IO](embedSvc, chunkRepo, versionRepo, testXa, testLogger, batchSize = 2)
      .use { embedder =>
        for {
          fiber1 <- embedder.submit(testCtx, Stream.emit("amend-1-chunk"), "ack-1", counters1.ack, counters1.nack).start
          fiber2 <- embedder
            .submit(testCtx2, Stream.emit("amend-2-chunk"), "ack-2", counters2.ack, counters2.nack)
            .start
          _ <- fiber1.joinWithNever
          _ <- fiber2.joinWithNever
        } yield ()
      }
      .timeout(TestTimeout)
      .unsafeRunSync()

    val _ = counters1.acks.get() shouldBe 1
    val _ = counters1.nacks.get() shouldBe 0
    val _ = counters2.acks.get() shouldBe 1
    val _ = counters2.nacks.get() shouldBe 0
    val _ = chunkRepo.rows.flatten.size shouldBe 2
    chunkRepo.rows.flatten.map(_.amendmentId).toSet shouldBe Set(42L, 99L)
  }

  it should "trim removes stale tail when re-submission produces fewer chunks (Q4)" in {
    // Sanity check that trimChunksPast is invoked with the new submission's chunk count. The stale-tail removal at
    // the DB layer is exercised in the integration spec; here we assert the embedder sends the right call.
    val embedSvc    = new RecordingEmbeddingService
    val chunkRepo   = new RecordingChunkRepo
    val versionRepo = new RecordingVersionRepo
    val counters    = new AckCounters

    val _ = CrossAmendmentEmbedder
      .resource[IO](embedSvc, chunkRepo, versionRepo, testXa, testLogger, batchSize = 50)
      .use(embedder => embedder.submit(testCtx, Stream("c0", "c1"), "ack-1", counters.ack, counters.nack))
      .timeout(TestTimeout)
      .unsafeRunSync()

    chunkRepo.trimCalls shouldBe Vector((7L, 2))
  }

  it should "NACK + re-raise when the producer's chunk stream itself raises before finalize" in {
    val embedSvc    = new RecordingEmbeddingService
    val chunkRepo   = new RecordingChunkRepo
    val versionRepo = new RecordingVersionRepo
    val counters    = new AckCounters

    val streamError                       = new java.io.IOException("upstream extractor blew up")
    val raisingStream: Stream[IO, String] = Stream.raiseError[IO](streamError)

    val raised: Either[Throwable, Unit] = CrossAmendmentEmbedder
      .resource[IO](embedSvc, chunkRepo, versionRepo, testXa, testLogger, batchSize = 50)
      .use(embedder => embedder.submit(testCtx, raisingStream, "ack-stream-fail", counters.ack, counters.nack))
      .timeout(TestTimeout)
      .attempt
      .unsafeRunSync()

    val _ = raised.left.toOption.map(_.getMessage) shouldBe Some("upstream extractor blew up")
    val _ = counters.acks.get() shouldBe 0
    val _ = counters.nacks.get() shouldBe 1
    val _ = chunkRepo.rows shouldBe empty
    versionRepo.markFetchedSeen shouldBe empty
  }

  it should "purge orphan buffered chunks for a failed ackId so they don't leak into a later flush" in {
    // Producer A emits 2 chunks (under batchSize=10, no flush yet) and then raises. `failAck` must remove
    // both buffered chunks before producer B's submission flushes — otherwise B's batch would carry A's
    // orphans (NACKed but still pending in the shared buffer) and persist them.
    val embedSvc    = new RecordingEmbeddingService
    val chunkRepo   = new RecordingChunkRepo
    val versionRepo = new RecordingVersionRepo
    val countersA   = new AckCounters
    val countersB   = new AckCounters

    val partialThenFail: Stream[IO, String] =
      Stream("orphan-0", "orphan-1") ++ Stream.raiseError[IO](new java.io.IOException("partway"))

    val _ = CrossAmendmentEmbedder
      .resource[IO](embedSvc, chunkRepo, versionRepo, testXa, testLogger, batchSize = 10)
      .use { embedder =>
        for {
          // First submission: 2 chunks then raises before any flush. A's stream error → failAck → NACK + buffer purge.
          _ <- embedder.submit(testCtx, partialThenFail, "ack-A", countersA.ack, countersA.nack).attempt
          // Second submission: 1 chunk, completes normally. Its forced residual flush MUST NOT include A's orphans.
          _ <- embedder.submit(testCtx2, Stream.emit("clean-chunk"), "ack-B", countersB.ack, countersB.nack)
        } yield ()
      }
      .timeout(TestTimeout)
      .unsafeRunSync()

    val _ = countersA.acks.get() shouldBe 0
    val _ = countersA.nacks.get() shouldBe 1
    val _ = countersB.acks.get() shouldBe 1
    val _ = countersB.nacks.get() shouldBe 0
    // Only one batch ever reached the embed service: B's single chunk. A's orphans were purged before any flush.
    val _ = embedSvc.batches.flatten shouldBe List("clean-chunk")
    val _ = chunkRepo.rows.flatten.map(_.content) shouldBe List("clean-chunk")
    chunkRepo.rows.flatten.map(_.amendmentId) shouldBe List(99L)
  }

  it should "NACK and clean up state when the producing fiber is cancelled mid-stream" in {
    // Without `guaranteeCase` the ackId would leak indefinitely on cancellation: `handleErrorWith` doesn't fire
    // for cancelled fibers. The new finalizer must invoke `failAck` so the ackId is removed and NACK fires.
    val embedSvc    = new RecordingEmbeddingService
    val chunkRepo   = new RecordingChunkRepo
    val versionRepo = new RecordingVersionRepo
    val counters    = new AckCounters

    val program: IO[Unit] = Deferred[IO, Unit].flatMap { chunkOffered =>
      val hangingStream: Stream[IO, String] =
        Stream.emit("chunk-0").covary[IO] ++
          Stream.eval(chunkOffered.complete(()).void).drain ++
          Stream.eval(IO.never[String])

      CrossAmendmentEmbedder
        .resource[IO](embedSvc, chunkRepo, versionRepo, testXa, testLogger, batchSize = 10)
        .use { embedder =>
          for {
            fiber <- embedder.submit(testCtx, hangingStream, "ack-cancel", counters.ack, counters.nack).start
            _     <- chunkOffered.get // chunk-0 was offered; producer is now hanging on IO.never
            _     <- fiber.cancel
            _     <- fiber.join.void  // wait for cancellation finalizer to complete
          } yield ()
        }
    }

    val _ = program.timeout(TestTimeout).unsafeRunSync()
    val _ = counters.acks.get() shouldBe 0
    val _ = counters.nacks.get() shouldBe 1
    // Nothing flushed — chunk-0 sat in the buffer, then was purged by failAck on cancel.
    val _ = chunkRepo.rows shouldBe empty
    versionRepo.markFetchedSeen shouldBe empty
  }

}
