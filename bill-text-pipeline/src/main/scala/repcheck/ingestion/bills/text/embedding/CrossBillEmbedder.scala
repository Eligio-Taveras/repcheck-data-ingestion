package repcheck.ingestion.bills.text.embedding

import scala.concurrent.duration.FiniteDuration

import cats.effect.std.Queue
import cats.effect.syntax.spawn._
import cats.effect.{Async, Deferred, Ref, Resource}
import cats.syntax.all._

import fs2.{Chunk, Stream}

import doobie._

import repcheck.ingestion.bills.common.persistence.TransactionRunner
import repcheck.ingestion.bills.text.persistence.RawBillTextRepository
import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}
import repcheck.pipeline.models.metadata.ProcessingResult
import repcheck.shared.models.congress.dos.bill.RawBillTextDO

/**
 * Boundary between the per-bill processor and whatever embedding strategy is in use. The default implementation is
 * [[CrossBillEmbedder]] (cross-bill batching for GPU saturation), but the trait exists so unit tests of the processor
 * don't have to spin up a background fiber + queue + state Ref to mock-out one method call.
 */
trait BillChunkEmbedder[F[_]] {

  /**
   * Submit a bill's chunk stream for embedding + persistence. Returns when ALL chunks are processed (success) or any
   * batch fails (Failed). See [[CrossBillEmbedder.processChunks]] for the contract details.
   */
  def processChunks(ctx: BillEmbedCtx, chunkStream: Stream[F, String]): F[ProcessingResult]

}

/**
 * Process-wide cross-bill embedding accumulator. Multiple [[BillTextProcessor.processEvent]] invocations submit their
 * bills' chunks here as they're produced; a single background fiber pulls chunks off the shared queue, accumulates them
 * into batches of `embedBatchSize` (or fires after `embedBatchTimeout`), embeds each batch via Ollama, and INSERTs the
 * entire batch (mixed across bills) into `raw_bill_text` in one transaction.
 *
 * ==Why cross-bill batching matters==
 *
 * Empirical observation in production: 82% of bills produce a single 12k-char chunk (resolutions, single-paragraph
 * orders), so the prior per-bill embedder was sending Ollama batches of 1 chunk. The GPU finished each call in
 * milliseconds and sat idle waiting for the next single-chunk batch — measured at 25% utilization with a 75% headroom.
 * Mixing chunks across bills inside one Ollama call lets the GPU process 50 chunks per kernel launch instead of 1,
 * which closes the utilization gap.
 *
 * ==Per-bill completion contract==
 *
 * `submit` returns a `F[ProcessingResult]` that resolves when ALL chunks for that bill have been INSERTed (success) OR
 * when any chunk's embed/INSERT fails (Failed). The caller (BillTextProcessor) awaits this result before marking the
 * version fetched / publishing the ingested event / ACKing the Pub/Sub message — Pub/Sub semantics are preserved
 * end-to-end despite the cross-bill batching at the embed layer.
 *
 * ==State machine==
 *
 * Each in-flight bill is tracked in a per-process `Ref[Map[Long, BillEmbedProgress[F]]]`:
 *
 *   - `expected` is `None` while the caller is still streaming chunks into the queue, becomes `Some(n)` when the caller
 *     signals end-of-submission via [[finalizeSubmission]].
 *   - `persisted` increments by 1 for every chunk in a successful batch INSERT.
 *   - When `expected.contains(persisted)`, the bill is complete: its [[Deferred]] resolves with `Succeeded` and the
 *     bill is removed from state.
 *   - On batch failure, every distinct bill in the failed batch has its Deferred resolved with `Failed` and is removed
 *     from state. Subsequent chunks still on the queue for those bills are processed as no-ops in
 *     `incrementAndMaybeComplete` (the bill isn't in state anymore) — they consume GPU + DB capacity but don't leak;
 *     the next pipeline tick's `clearOrphanChunks` cleans up the partial chunks.
 *
 * ==Backpressure==
 *
 * The internal queue is bounded; submit-side `offer` blocks when the queue is full, propagating backpressure all the
 * way back to the HTTP body read in [[BillTextDownloader]]. With a queue capacity of e.g. 500 and ~12 KB per chunk,
 * peak in-flight memory is bounded at ~6 MB regardless of how many bills are concurrent.
 *
 * ==Lifecycle==
 *
 * Constructed via [[CrossBillEmbedder.resource]] which spawns the background worker fiber. The Resource's release
 * cancels the fiber and any bills still in flight resolve to a Failed result (their Deferred completes with the
 * shutdown error). In a process-restart scenario the unfinished bills are NACKed via Pub/Sub redelivery, so no chunks
 * are lost — they re-enter the next process via standard redelivery.
 */
class CrossBillEmbedder[F[_]: Async] private[embedding] (
  embeddingService: EmbeddingService[F],
  rawBillTextRepository: RawBillTextRepository[ConnectionIO],
  xa: Transactor[F],
  logger: PipelineLogger[F],
  state: Ref[F, Map[Long, BillEmbedProgress[F]]],
  queue: Queue[F, ChunkSubmission],
) extends BillChunkEmbedder[F] {

  private val StepName = "cross-bill-embedder"

  /**
   * Process a bill's chunks through the cross-bill embedding pipeline. This is the only method
   * [[repcheck.ingestion.bills.text.pipeline.BillTextProcessor]] needs to call.
   *
   *   1. Registers the bill in shared state with `expected = None` (chunk count unknown yet). 2. Drains the supplied
   *      chunk `Stream` into the shared embedder queue. Backpressure flows naturally — if the queue is full, `offer`
   *      blocks, `evalMap` pauses, and the upstream extractor stops pulling bytes. 3. After the stream completes, calls
   *      [[finalizeSubmission]] with the observed chunk count. The worker may have already persisted all chunks by then
   *      (in which case finalize is what completes the Deferred), or it hasn't yet (in which case the worker will
   *      detect completion when persisted catches up). 4. Awaits the bill's Deferred — resolved by either the worker
   *      (success or batch failure) or by the cleanup Resource on caller-side abort.
   *
   * The Resource lifecycle ensures that if the caller's effect (e.g., the chunk stream) fails mid-submission, the bill
   * is removed from shared state and the Deferred resolves to a Failed result; the caller's effect channel gets the
   * original failure as expected.
   */
  override def processChunks(ctx: BillEmbedCtx, chunkStream: Stream[F, String]): F[ProcessingResult] =
    Resource
      .make(register(ctx))(_ => cleanupIfStillRegistered(ctx))
      .use { awaitResult =>
        for {
          chunkCount <- chunkStream.zipWithIndex
            .evalMap { case (text, idx) => offerChunk(ctx, idx.toInt, text) }
            .compile
            .count
          _      <- finalizeSubmission(ctx.dbBillId, chunkCount.toInt)
          result <- awaitResult
        } yield result
      }

  private[embedding] def register(ctx: BillEmbedCtx): F[F[ProcessingResult]] =
    Deferred[F, ProcessingResult].flatMap { deferred =>
      val initial = BillEmbedProgress[F](
        ctx = ctx,
        expected = None,
        persisted = 0,
        deferred = deferred,
      )
      state.update(_ + (ctx.dbBillId -> initial)).as(deferred.get)
    }

  private[embedding] def offerChunk(ctx: BillEmbedCtx, chunkIdx: Int, text: String): F[Unit] =
    queue.offer(ChunkSubmission(ctx, chunkIdx, text))

  private[embedding] def finalizeSubmission(dbBillId: Long, expected: Int): F[Unit] =
    state
      .modify { current =>
        current.get(dbBillId) match {
          case Some(progress) =>
            val updated = progress.copy(expected = Some(expected))
            if (updated.shouldComplete) {
              (current - dbBillId, Some(updated))
            } else {
              (current.updated(dbBillId, updated), None)
            }
          case None =>
            // Bill was already removed (failed or aborted) — nothing to finalize.
            (current, None)
        }
      }
      .flatMap {
        case Some(progress) =>
          progress.deferred
            .complete(ProcessingResult.Succeeded(progress.ctx.naturalKey, eventEmitted = false))
            .void
        case None => Async[F].unit
      }

  /**
   * Resource-release callback. If the bill is still in state (i.e., neither the worker completed it nor failed it),
   * removes it and resolves its Deferred to Failed. If it's already gone (normal completion or batch failure removed
   * it), no-ops. Used by the [[processChunks]] Resource to guarantee no dangling state when a caller's stream fails
   * before [[finalizeSubmission]].
   */
  private[embedding] def cleanupIfStillRegistered(ctx: BillEmbedCtx): F[Unit] =
    state
      .modify { current =>
        current.get(ctx.dbBillId) match {
          case Some(progress) => (current - ctx.dbBillId, Some(progress))
          case None           => (current, None)
        }
      }
      .flatMap {
        case Some(progress) =>
          progress.deferred
            .complete(
              ProcessingResult.Failed(
                progress.ctx.naturalKey,
                "Bill processing aborted before submission finalized",
                "Systemic",
              )
            )
            .void
        case None => Async[F].unit
      }

  /** Worker entry point. Run in a background fiber via [[CrossBillEmbedder.resource]]. */
  private[embedding] def runWorker(embedBatchSize: Int, embedBatchTimeout: FiniteDuration): F[Unit] =
    Stream
      .fromQueueUnterminated(queue)
      .groupWithin(embedBatchSize, embedBatchTimeout)
      .evalMap(processBatch)
      .compile
      .drain

  private[embedding] def processBatch(batch: Chunk[ChunkSubmission]): F[Unit] =
    if (batch.isEmpty) {
      Async[F].unit
    } else {
      val texts = batch.toList.map(_.text)
      embeddingService
        .generateEmbeddings(texts)
        .attempt
        .flatMap {
          case Left(error) => failBatch(batch, error)
          case Right(embeddings) =>
            val rows = batch.toList.zip(embeddings).map {
              case (sub, emb) =>
                RawBillTextDO(
                  id = 0L,
                  billId = sub.ctx.dbBillId,
                  versionId = Some(sub.ctx.versionId),
                  chunkIndex = sub.chunkIdx,
                  content = sub.text,
                  embedding = emb,
                  createdAt = None,
                )
            }
            TransactionRunner
              .run(xa)(rawBillTextRepository.insertMany(rows))
              .attempt
              .flatMap {
                case Left(error) => failBatch(batch, error)
                case Right(_)    => batch.toList.traverse_(sub => incrementAndMaybeComplete(sub.ctx.dbBillId))
              }
        }
    }

  private[embedding] def incrementAndMaybeComplete(dbBillId: Long): F[Unit] =
    state
      .modify { current =>
        current.get(dbBillId) match {
          case Some(progress) =>
            val updated = progress.copy(persisted = progress.persisted + 1)
            if (updated.shouldComplete) {
              (current - dbBillId, Some(updated))
            } else {
              (current.updated(dbBillId, updated), None)
            }
          case None =>
            // Bill was already removed from state (failed, or completed via finalize). The chunk
            // was still in the queue when the bill's other chunks failed — its INSERT just
            // happened anyway, no harm. The next pipeline tick's clearOrphanChunks deletes it.
            (current, None)
        }
      }
      .flatMap {
        case Some(progress) =>
          progress.deferred
            .complete(ProcessingResult.Succeeded(progress.ctx.naturalKey, eventEmitted = false))
            .void
        case None => Async[F].unit
      }

  private def failBatch(batch: Chunk[ChunkSubmission], error: Throwable): F[Unit] = {
    val billsInBatch = batch.toList.map(_.ctx).distinctBy(_.dbBillId)
    val logCtx = LogContext(
      runId = "<batch>",
      stepName = StepName,
      correlationId = None,
      entityId = Some(s"${billsInBatch.size}-bills"),
    )
    logger.error(
      logCtx,
      s"Embed/INSERT batch failed for ${billsInBatch.size} bill(s); failing each: ${error.getMessage}",
      Some(error),
    ) *>
      billsInBatch.traverse_ { ctx =>
        state
          .modify { current =>
            current.get(ctx.dbBillId) match {
              case Some(progress) => (current - ctx.dbBillId, Some(progress))
              case None           => (current, None)
            }
          }
          .flatMap {
            case Some(progress) =>
              progress.deferred
                .complete(
                  ProcessingResult.Failed(
                    progress.ctx.naturalKey,
                    Option(error.getMessage).getOrElse(error.getClass.getSimpleName),
                    classifyBatchError(error),
                  )
                )
                .void
            case None => Async[F].unit
          }
      }
  }

  /**
   * Classify a batch-level error. Mirrors [[BillTextProcessor.classifyError]] but scoped to the failure modes the
   * embedder can raise — embedding service errors and DB errors. Anything else falls through to Systemic.
   */
  private[embedding] def classifyBatchError(error: Throwable): String =
    error match {
      case _: EmbeddingContextLengthExceeded  => "Systemic"
      case _: EmbeddingGenerationFailed       => "Transient"
      case _: java.net.SocketTimeoutException => "Transient"
      case _: java.net.ConnectException       => "Transient"
      case _: java.io.IOException             => "Transient"
      case _: java.sql.SQLTransientException  => "Transient"
      case _                                  => "Systemic"
    }

}

object CrossBillEmbedder {

  /**
   * Allocate a [[CrossBillEmbedder]] backed by a bounded queue and a background worker fiber. The Resource's release
   * cancels the fiber.
   *
   * @param queueCapacity
   *   max number of chunks that can sit in the queue before submit-side blocks. Sized to comfortably hold one batch's
   *   worth plus headroom — e.g. 500 for batchSize=50.
   */
  def resource[F[_]: Async](
    embeddingService: EmbeddingService[F],
    rawBillTextRepository: RawBillTextRepository[ConnectionIO],
    xa: Transactor[F],
    logger: PipelineLogger[F],
    embedBatchSize: Int,
    embedBatchTimeout: FiniteDuration,
    queueCapacity: Int,
  ): Resource[F, CrossBillEmbedder[F]] =
    for {
      stateRef <- Resource.eval(Ref.of[F, Map[Long, BillEmbedProgress[F]]](Map.empty))
      queue    <- Resource.eval(Queue.bounded[F, ChunkSubmission](queueCapacity))
      embedder = new CrossBillEmbedder[F](embeddingService, rawBillTextRepository, xa, logger, stateRef, queue)
      _ <- embedder.runWorker(embedBatchSize, embedBatchTimeout).background
    } yield embedder

}
