package repcheck.ingestion.bills.text.embedding

import cats.effect.{Async, Deferred, Ref, Resource}
import cats.syntax.all._

import doobie._

import fs2.Stream

import repcheck.ingestion.bills.common.persistence.TransactionRunner
import repcheck.ingestion.bills.text.persistence.RawBillTextRepository
import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}
import repcheck.pipeline.models.metadata.ProcessingResult
import repcheck.shared.models.congress.dos.bill.RawBillTextDO

/**
 * Boundary between the per-bill processor and whatever embedding strategy is in use. The default implementation is
 * [[CrossBillEmbedder]] (cross-bill batching for GPU saturation), but the trait exists so unit tests of the processor
 * don't have to spin up the full state machine just to mock-out one method call.
 */
trait BillChunkEmbedder[F[_]] {

  /**
   * Submit a bill's chunk stream for embedding + persistence. Returns when ALL chunks are processed (success) or any
   * flush containing one of this bill's chunks fails (Failed). See [[CrossBillEmbedder.processChunks]] for the contract
   * details.
   */
  def processChunks(ctx: BillEmbedCtx, chunkStream: Stream[F, String]): F[ProcessingResult]

}

/**
 * Process-wide cross-bill embedding accumulator. Multiple [[repcheck.ingestion.bills.text.pipeline.BillTextProcessor]]
 * invocations (one per bill, running concurrently in the outer pipeline's `parEvalMap`) submit chunks here. Chunks
 * accumulate in a shared buffer until any producer's offer fills it to `batchSize`, at which point THAT PRODUCER
 * synchronously embeds + persists the entire buffer (mixed across bills) and resolves the affected bills' Deferreds.
 *
 * ==Foreground-only design==
 *
 * There is NO background fiber. Every embedding + DB write happens on the producing fiber that triggered the flush
 * (either by filling the buffer to `batchSize` in [[offerChunk]], or by force-flushing the residual buffer in
 * [[finalizeSubmission]]). Errors propagate through that fiber's normal IO channel and surface in the bill's
 * `processChunks` result — no `.background` Outcome to swallow them, no Deferred orphaning if a worker fiber dies.
 *
 * The earlier background-fiber design hung in production: the worker fiber suspended in some async call without
 * raising, which meant the registered Deferreds never resolved, and `parEvalMap` slots stayed locked indefinitely.
 * Moving to a fully-foreground state machine eliminates that class of bug entirely.
 *
 * ==Per-bill completion contract==
 *
 * `processChunks` returns an `F[ProcessingResult]` that resolves when ALL of THIS bill's chunks have been INSERTed
 * (success) OR when any flush containing one of this bill's chunks fails (Failed). The caller (BillTextProcessor)
 * awaits this result before marking the version fetched / publishing the ingested event / ACKing the Pub/Sub message.
 *
 * ==Why cross-bill batching matters==
 *
 * Empirical observation in production: 82% of bills produce a single 12k-char chunk (resolutions, single-paragraph
 * orders), so the prior per-bill embedder was sending Ollama batches of 1 chunk. The GPU finished each call in
 * milliseconds and sat idle waiting for the next single-chunk batch — measured at 25% utilization with a 75% headroom.
 * Mixing chunks across bills inside one Ollama call lets the GPU process up to `batchSize` chunks per kernel launch
 * instead of 1, which closes the utilization gap.
 *
 * ==State transitions==
 *
 * Each in-flight bill is tracked in `state.bills: Map[Long, BillEmbedProgress[F]]` with:
 *   - `expected: Option[Int]` — set to `None` while the producer streams chunks; set to `Some(n)` by
 *     `finalizeSubmission` after the chunk stream terminates with `n` chunks observed.
 *   - `persisted: Int` — incremented atomically inside the buffer-flush transaction whenever a chunk for this bill is
 *     INSERTed.
 *   - When `expected.contains(persisted)`, the bill is complete. Its Deferred is resolved with `Succeeded` and it is
 *     removed from `state.bills`.
 *
 * On flush failure, every distinct bill in the failed batch is removed from state and its Deferred resolved with
 * `Failed`. Subsequent chunks for those bills are still in `state.buffer` (or arrive later) and would normally
 * increment `persisted`; the `applyBatchResult` step handles "bill no longer in state" as a no-op so leftover
 * post-failure flushes don't crash. Stale rows in `raw_bill_text` are cleaned up by the next pipeline tick's
 * `clearOrphanChunks` for the bill's `versionId`.
 */
class CrossBillEmbedder[F[_]: Async] private[embedding] (
  embeddingService: EmbeddingService[F],
  rawBillTextRepository: RawBillTextRepository[ConnectionIO],
  xa: Transactor[F],
  logger: PipelineLogger[F],
  state: Ref[F, EmbedderState[F]],
  batchSize: Int,
) extends BillChunkEmbedder[F] {

  private val StepName = "cross-bill-embedder"

  /**
   * Process a bill's chunks through the cross-bill embedding pipeline.
   *
   *   1. `register` reserves a Deferred for the bill in shared state. 2. The chunk stream is drained through
   *      `offerChunk`, which atomically buffers each chunk; if a particular offer fills the buffer to `batchSize`, that
   *      offer also performs the flush (embed + insertMany + per-bill progress updates + completion of any bills whose
   *      chunks were entirely processed). 3. `finalizeSubmission` is called after the chunk stream terminates, with the
   *      observed chunk count. It sets `expected` and force-flushes the residual buffer (if any) — this is what
   *      guarantees the LAST bill's chunks get processed even if no further producer arrives to fill the buffer. 4.
   *      `awaitResult` blocks until the bill's Deferred resolves.
   *
   * The Resource lifecycle ensures that if the caller's effect (e.g. the chunk stream) fails mid-submission, the bill
   * is removed from shared state and its Deferred resolves to a Failed result; the caller's effect channel still gets
   * the original failure.
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
          _      <- finalizeSubmission(ctx, chunkCount.toInt)
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
      state.update(s => s.copy(bills = s.bills + (ctx.dbBillId -> initial))).as(deferred.get)
    }

  /**
   * Atomically add a chunk to the shared buffer. If this offer brings the buffer to `batchSize`, the producer takes
   * responsibility for flushing — embeds the batch, persists rows, updates per-bill progress, completes any newly
   * completed bills' Deferreds. Other producers continue offering during the flush; they may trigger their own
   * subsequent flushes on later offers.
   */
  private[embedding] def offerChunk(ctx: BillEmbedCtx, chunkIdx: Int, text: String): F[Unit] = {
    val submission = ChunkSubmission(ctx, chunkIdx, text)
    state
      .modify { s =>
        val newBuffer = s.buffer :+ submission
        if (newBuffer.size >= batchSize) {
          (s.copy(buffer = Vector.empty), newBuffer.toList)
        } else {
          (s.copy(buffer = newBuffer), List.empty)
        }
      }
      .flatMap(flushIfNonEmpty)
  }

  /**
   * Set `expected` for a bill (so it can complete once `persisted` catches up) AND atomically take the residual buffer
   * for force-flushing. Two cases:
   *
   *   - If the bill's chunks are already all processed (persisted == count), the bill's Deferred is resolved
   *     immediately with Succeeded and any residual buffer is ALSO flushed (other bills' chunks may need it).
   *   - If the bill is not yet complete, the residual buffer is flushed (which may complete this bill or others), and
   *     the bill awaits its Deferred until either this flush or a future producer's flush completes it.
   *
   * Empty-stream case (count = 0): the bill has expected = Some(0) and persisted = 0 → `shouldComplete` is true and the
   * bill's Deferred resolves to Succeeded immediately.
   */
  private[embedding] def finalizeSubmission(ctx: BillEmbedCtx, count: Int): F[Unit] =
    state
      .modify { s =>
        s.bills.get(ctx.dbBillId) match {
          case Some(progress) =>
            val updated = progress.copy(expected = Some(count))
            val flush   = s.buffer.toList
            val newBills =
              if (updated.shouldComplete) s.bills - ctx.dbBillId else s.bills.updated(ctx.dbBillId, updated)
            val maybeDone = if (updated.shouldComplete) Some(updated) else None
            (s.copy(buffer = Vector.empty, bills = newBills), (maybeDone, flush))
          case None =>
            // Bill was already removed (e.g. failed or aborted) — nothing to finalize.
            (s, (None, List.empty[ChunkSubmission]))
        }
      }
      .flatMap {
        case (maybeCompleted, residual) =>
          maybeCompleted.traverse_ { progress =>
            progress.deferred
              .complete(ProcessingResult.Succeeded(progress.ctx.naturalKey, eventEmitted = false))
              .void
          } *> flushIfNonEmpty(residual)
      }

  /**
   * Resource-release callback. If the bill is still in state (i.e. neither a flush completed it nor a flush failed it),
   * removes it and resolves its Deferred to Failed. Used by [[processChunks]]'s Resource to guarantee no dangling state
   * when the caller's chunk stream fails before [[finalizeSubmission]].
   */
  private[embedding] def cleanupIfStillRegistered(ctx: BillEmbedCtx): F[Unit] =
    state
      .modify { s =>
        s.bills.get(ctx.dbBillId) match {
          case Some(progress) =>
            (s.copy(bills = s.bills - ctx.dbBillId), Some(progress))
          case None => (s, None)
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

  /**
   * Embed + persist a batch in a single foreground step, then atomically update per-bill progress (incrementing
   * `persisted` for each chunk's bill, completing any newly completed bills, removing them from state).
   *
   * Errors raised by either `embeddingService.generateEmbeddings` or `rawBillTextRepository.insertMany` propagate
   * through `attempt`. On error: every distinct bill in the failed batch has its Deferred resolved with Failed and is
   * removed from state. The error itself is NOT re-raised — the calling fiber (a producer) shouldn't fail just because
   * some other bill's flush errored. The affected bills' processChunks calls observe the Failed result via their
   * Deferreds.
   */
  private[embedding] def flushIfNonEmpty(batch: List[ChunkSubmission]): F[Unit] =
    if (batch.isEmpty) {
      Async[F].unit
    } else {
      val texts = batch.map(_.text)
      embeddingService
        .generateEmbeddings(texts)
        .attempt
        .flatMap {
          case Left(error) => failBatch(batch, error)
          case Right(embeddings) =>
            val rows = batch.zip(embeddings).map {
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
                case Right(_)    => applyBatchResult(batch)
              }
        }
    }

  /**
   * Atomically increment `persisted` for each bill represented in the batch and remove fully-completed bills from
   * state. Returns the list of completed bill progresses so we can resolve their Deferreds (outside the Ref transaction
   * since `Deferred.complete` should not run under a CAS retry loop).
   */
  private[embedding] def applyBatchResult(batch: List[ChunkSubmission]): F[Unit] = {
    val perBillCounts: Map[Long, Int] = batch.groupMapReduce(_.ctx.dbBillId)(_ => 1)(_ + _)
    state
      .modify { s =>
        perBillCounts.foldLeft((s, List.empty[BillEmbedProgress[F]])) {
          case ((acc, completed), (billId, count)) =>
            acc.bills.get(billId) match {
              case Some(progress) =>
                val updated = progress.copy(persisted = progress.persisted + count)
                if (updated.shouldComplete) {
                  (acc.copy(bills = acc.bills - billId), updated :: completed)
                } else {
                  (acc.copy(bills = acc.bills.updated(billId, updated)), completed)
                }
              case None =>
                // Bill was removed (e.g. by a prior failBatch). The chunks still got embedded + INSERTed; the
                // orphan rows are cleaned up by the next pipeline tick's `clearOrphanChunks` for that versionId.
                (acc, completed)
            }
        }
      }
      .flatMap { completed =>
        completed.traverse_ { progress =>
          progress.deferred
            .complete(ProcessingResult.Succeeded(progress.ctx.naturalKey, eventEmitted = false))
            .void
        }
      }
  }

  /**
   * On batch-level error (embedding service raised, or DB raised): atomically remove every distinct bill in the batch
   * from state and resolve their Deferreds with Failed. Logs the cause at ERROR. Errors from logging /
   * Deferred.complete itself are not re-raised — the caller is one of many producers and we don't want to crash the
   * producer that happened to be the flusher.
   */
  private[embedding] def failBatch(batch: List[ChunkSubmission], error: Throwable): F[Unit] = {
    val billsInBatch = batch.map(_.ctx).distinctBy(_.dbBillId)
    val logCtx = LogContext(
      runId = "<batch>",
      stepName = StepName,
      correlationId = None,
      entityId = Some(s"${billsInBatch.size}-bills"),
    )
    val errorMessage = Option(error.getMessage).getOrElse(error.getClass.getSimpleName)
    logger.error(
      logCtx,
      s"Embed/INSERT batch failed for ${billsInBatch.size} bill(s); failing each: $errorMessage",
      Some(error),
    ) *>
      state
        .modify { s =>
          billsInBatch.foldLeft((s, List.empty[BillEmbedProgress[F]])) {
            case ((acc, removed), ctx) =>
              acc.bills.get(ctx.dbBillId) match {
                case Some(progress) => (acc.copy(bills = acc.bills - ctx.dbBillId), progress :: removed)
                case None           => (acc, removed)
              }
          }
        }
        .flatMap { removed =>
          removed.traverse_ { progress =>
            progress.deferred
              .complete(
                ProcessingResult.Failed(progress.ctx.naturalKey, errorMessage, classifyBatchError(error))
              )
              .void
          }
        }
  }

  /**
   * Classify a batch-level error. Mirrors [[repcheck.ingestion.bills.text.pipeline.BillTextProcessor.classifyError]]
   * but scoped to the failure modes the embedder can raise — embedding-service errors and DB errors.
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
   * Allocate a [[CrossBillEmbedder]]. Unlike the previous background-fiber design, this Resource holds no fibers — the
   * only persistent state is a `Ref` for the shared buffer + per-bill progress map. Resource release is effectively a
   * no-op; any in-flight bills' processChunks calls own their own Deferreds and will resolve them (or surface a Failed
   * via `cleanupIfStillRegistered` if their stream fails).
   *
   * @param batchSize
   *   the buffer threshold that triggers a producer-driven flush. Match this to `OLLAMA_EMBED_BATCH_SIZE` so flushes
   *   correspond to the GPU's preferred batch size.
   */
  def resource[F[_]: Async](
    embeddingService: EmbeddingService[F],
    rawBillTextRepository: RawBillTextRepository[ConnectionIO],
    xa: Transactor[F],
    logger: PipelineLogger[F],
    batchSize: Int,
  ): Resource[F, CrossBillEmbedder[F]] =
    Resource.eval(Ref.of[F, EmbedderState[F]](EmbedderState.empty[F])).map { state =>
      new CrossBillEmbedder[F](embeddingService, rawBillTextRepository, xa, logger, state, batchSize)
    }

}
