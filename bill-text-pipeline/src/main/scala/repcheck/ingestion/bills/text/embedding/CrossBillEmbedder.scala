package repcheck.ingestion.bills.text.embedding

import java.time.Instant

import cats.effect.{Async, Outcome, Ref, Resource}
import cats.syntax.all._

import fs2.Stream

import doobie._

import repcheck.ingestion.bills.common.persistence.{BillTextVersionRepository, TransactionRunner}
import repcheck.ingestion.bills.text.persistence.RawBillTextRepository
import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}
import repcheck.ingestion.text.embedding.EmbeddingService
import repcheck.shared.models.congress.dos.bill.RawBillTextDO

/**
 * Boundary between the per-bill processor and whatever embedding strategy is in use. The default implementation is
 * [[CrossBillEmbedder]] (cross-bill batching for GPU saturation), but the trait exists so unit tests of the processor
 * don't have to spin up the full state machine just to mock-out one method call.
 */
trait BillChunkEmbedder[F[_]] {

  /**
   * Submit a bill's chunk stream for embedding + persistence. Fire-and-forget: returns once the chunks have been
   * enqueued and the residual buffer flushed, NOT once they've persisted. The embedder owns the rest of the lifecycle:
   *
   *   - chunk persistence (UPSERT into `raw_bill_text`)
   *   - trim of any stale tail past this submission's chunk count
   *   - `markFetched` on `bill_text_versions`
   *   - invoking `ack` (success) or `nack` (failure) on the Pub/Sub message
   *
   * @param ctx
   *   identifying info (dbBillId, versionId, naturalKey).
   * @param chunkStream
   *   the chunks to persist for this submission. May be empty (ack fires immediately).
   * @param ackId
   *   the Pub/Sub ack id of the message that produced this submission.
   * @param ack
   *   effect to invoke once every chunk this submission offered has been processed (success or recorded failure).
   * @param nack
   *   effect to invoke if any flush containing one of this submission's chunks fails (or the chunkStream itself
   *   raises). Pub/Sub will redeliver; bounded retries via subscription `max_delivery_attempts` + dead-letter topic.
   */
  def submit(
    ctx: BillEmbedCtx,
    chunkStream: Stream[F, String],
    ackId: String,
    ack: F[Unit],
    nack: F[Unit],
  ): F[Unit]

}

/**
 * Process-wide cross-bill embedding accumulator. Multiple [[repcheck.ingestion.bills.text.pipeline.BillTextProcessor]]
 * invocations (one per bill, running concurrently in the outer pipeline's `parEvalMap`) submit chunks here. Chunks
 * accumulate in a shared buffer until any producer's offer fills it to `batchSize`, at which point THAT PRODUCER
 * synchronously embeds + persists the entire buffer (mixed across bills) and ACKs / NACKs the affected Pub/Sub
 * messages.
 *
 * ==Foreground-only design==
 *
 * There is NO background fiber. Every embedding + DB write happens on the producing fiber that triggered the flush
 * (either by filling the buffer to `batchSize` in [[offerChunk]], or by force-flushing the residual buffer in
 * [[finalizeSubmission]]).
 *
 * Failure propagation is one-way (NACK → Pub/Sub-redelivery), NOT through `submit`'s return effect:
 *
 *   - Flush errors (embed or UPSERT raised): absorbed by `flushIfNonEmpty`'s `attempt`. `failBatch` removes every ackId
 *     in the failed batch from state, purges any of their buffered submissions, and invokes `nack` for each. The
 *     producer that happened to be the flusher returns successfully — its own ackId is just one of N that got NACKed,
 *     and surfacing the error to the calling fiber would crash an unrelated bill's submit.
 *   - Stream errors (chunkStream itself raised): handled by `submit`'s `guaranteeCase` finalizer routing through
 *     `cleanupOnSubmitError` (purge buffer + NACK + remove ackId). The error then re-raises so the producer's effect
 *     channel still surfaces it for caller-side observability.
 *   - Producer cancellation (fiber cancelled — graceful shutdown, supervisor cancel): same `guaranteeCase` finalizer
 *     fires for `Outcome.Canceled`. Without it, the ackId would sit in `acks` indefinitely and chunks could orphan in
 *     the buffer.
 *
 * ==Per-ackId state — Option C (no version-date gate)==
 *
 *   - `AckProgress` per ackId: `(ack, nack, expected, submitted, written)`
 *   - `submitted` drives ACK: when `expected.contains(submitted)`, every chunk this producer offered has been processed
 *     by some flush (success or recorded failure) and `ack` fires.
 *   - `written` drives trim + markFetched: only when `written > 0` (the submission actually wrote rows) does the
 *     embedder run `trimChunksPast(versionId, chunkCount)` then `markFetched(versionId, NOW())`.
 *   - On flush failure: every ackId in the batch is removed from state, their buffered submissions are purged, and
 *     `nack` fires for each. Pub/Sub redelivers; the next attempt's UPSERT is idempotent (last-writer-wins on
 *     `(version_id, chunk_index)`).
 *
 * ==Why no version-date gate==
 *
 * `BillTextAvailableEvent` does not carry a `versionDate` field, and `bill_text_versions.version_date` is always
 * written as `None` today. The amendments side runs a version-date-gated UPSERT to filter older redeliveries at the SQL
 * layer; bills cannot, so it falls back to last-writer-wins. This is safe in practice because Congress.gov text for a
 * given `(billId, versionCode)` is monotonic — re-emissions with the same version code rewrite identical data.
 */
class CrossBillEmbedder[F[_]: Async] private[embedding] (
  embeddingService: EmbeddingService[F],
  rawBillTextRepository: RawBillTextRepository[ConnectionIO],
  textVersionRepository: BillTextVersionRepository[ConnectionIO],
  xa: Transactor[F],
  logger: PipelineLogger[F],
  state: Ref[F, EmbedderState[F]],
  batchSize: Int,
) extends BillChunkEmbedder[F] {

  private val StepName = "cross-bill-embedder"

  override def submit(
    ctx: BillEmbedCtx,
    chunkStream: Stream[F, String],
    ackId: String,
    ack: F[Unit],
    nack: F[Unit],
  ): F[Unit] = {
    val producer: F[Unit] =
      register(ctx, ackId, ack, nack) *> {
        for {
          chunkCount <- chunkStream.zipWithIndex
            .evalMap { case (text, idx) => offerChunk(ctx, idx.toInt, text, ackId) }
            .compile
            .count
          _ <- finalizeSubmission(ackId, chunkCount.toInt)
        } yield ()
      }

    // `guaranteeCase` covers all three exit paths so the ackId never leaks in the state map:
    //   - Errored: chunk stream raised (e.g., upstream extractor blew up) — purge buffer + NACK + re-raise.
    //   - Canceled: fiber cancelled (graceful shutdown / supervisor cancel) — purge buffer + NACK so Pub/Sub
    //     redelivers; otherwise the entry would sit in `acks` indefinitely and chunks would orphan in the buffer.
    //   - Succeeded: completion fires via `applyBatchSuccess`/`finalizeSubmission` paths; nothing to do here.
    Async[F].guaranteeCase(producer) {
      case Outcome.Succeeded(_) => Async[F].unit
      case Outcome.Errored(e)   => cleanupOnSubmitError(ackId, e)
      case Outcome.Canceled()   => cleanupOnSubmitError(ackId, BillSubmissionCancelled(ackId))
    }
  }

  private[embedding] def register(
    ctx: BillEmbedCtx,
    ackId: String,
    ack: F[Unit],
    nack: F[Unit],
  ): F[Unit] = {
    val initial = AckProgress[F](
      ackId = ackId,
      ctx = ctx,
      ack = ack,
      nack = nack,
      expected = None,
      submitted = 0,
      written = 0,
    )
    state.update(s => s.copy(acks = s.acks + (ackId -> initial)))
  }

  /**
   * Atomically add a chunk to the shared buffer iff its `ackId` is still registered. If the ackId was already removed
   * by a prior `failBatch`/`cleanupOnSubmitError` (e.g., an earlier batch's flush errored or this producer's stream is
   * mid-cleanup), the chunk is dropped — there's no longer anyone to attribute its write to or fire ACK for. The
   * producer's own loop will exit via `submit`'s `guaranteeCase` finalizer; no special signaling needed here.
   *
   * If the offer brings the buffer to `batchSize`, the producer takes responsibility for flushing — embeds the batch,
   * UPSERTs rows, updates per-ackId progress, runs trim + markFetched + ack for any ackIds whose chunks all got
   * processed.
   */
  private[embedding] def offerChunk(ctx: BillEmbedCtx, chunkIdx: Int, text: String, ackId: String): F[Unit] = {
    val submission = ChunkSubmission(ctx, chunkIdx, text, ackId)
    state
      .modify { s =>
        if (!s.acks.contains(ackId)) {
          (s, List.empty[ChunkSubmission])
        } else {
          val newBuffer = s.buffer :+ submission
          if (newBuffer.size >= batchSize) {
            (s.copy(buffer = Vector.empty), newBuffer.toList)
          } else {
            (s.copy(buffer = newBuffer), List.empty[ChunkSubmission])
          }
        }
      }
      .flatMap(flushIfNonEmpty)
  }

  /**
   * Set `expected` for an ackId AND atomically take the residual buffer for force-flushing. Two cases:
   *
   *   - Empty stream (`count = 0`): the ackId has `expected = Some(0)` and `submitted = 0` → it's immediately ack-able.
   *     We invoke `ack` and remove it from state. Trim + markFetched are SKIPPED because `written = 0`.
   *   - Non-empty stream: residual buffer (which may contain this ackId's chunks AND chunks from other bills) is
   *     flushed; whichever flush brings `submitted == expected` for this ackId triggers the ack + trim + markFetched.
   */
  private[embedding] def finalizeSubmission(ackId: String, count: Int): F[Unit] =
    state
      .modify { s =>
        s.acks.get(ackId) match {
          case Some(progress) =>
            val updated = progress.copy(expected = Some(count))
            val flush   = s.buffer.toList
            val maybeImmediate =
              if (updated.shouldAck) Some(updated) else None
            val newAcks =
              if (updated.shouldAck) s.acks - ackId else s.acks.updated(ackId, updated)
            (s.copy(buffer = Vector.empty, acks = newAcks), (maybeImmediate, flush))
          case None =>
            // ackId already removed (e.g., a prior flush failed it). Nothing to finalize.
            (s, (None, List.empty[ChunkSubmission]))
        }
      }
      .flatMap {
        case (maybeImmediate, residual) =>
          completeAcks(maybeImmediate.toList) *> flushIfNonEmpty(residual)
      }

  /**
   * Embed + persist a batch in a single foreground step, then atomically update per-ackId progress (incrementing
   * `submitted` and `written` for each ackId, collecting any ackIds whose `submitted` now equals `expected`, removing
   * them from state, and invoking trim + markFetched + ack for each).
   *
   * Errors raised by either `embeddingService.generateEmbeddings` or `rawBillTextRepository.upsertMany` propagate
   * through `attempt`. On error: every distinct ackId in the failed batch is removed from state and NACKed. The error
   * itself is NOT re-raised — the calling fiber (a producer) shouldn't fail just because some other ackId's flush
   * errored. The affected ackIds' submissions observe the failure via the NACK→Pub/Sub-redelivery path.
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
              .run(xa)(rawBillTextRepository.upsertMany(rows))
              .attempt
              .flatMap {
                case Left(error) => failBatch(batch, error)
                // affected rows ignored by attribution: on success the UPSERT is all-or-nothing (an idempotent
                // INSERT-or-UPDATE per row), so per-ackId `written` matches per-ackId `submitted` for this batch.
                case Right(_) => applyBatchSuccess(batch)
              }
        }
    }

  /**
   * Atomically credit each ackId in the batch with its chunk count for both `submitted` and `written`, then remove any
   * newly ack-able ackIds from state and run their trim + markFetched + ack outside the Ref transaction.
   */
  private[embedding] def applyBatchSuccess(batch: List[ChunkSubmission]): F[Unit] = {
    val perAckCounts: Map[String, Int] = batch.groupMapReduce(_.ackId)(_ => 1)(_ + _)
    state
      .modify { s =>
        perAckCounts.foldLeft((s, List.empty[AckProgress[F]])) {
          case ((acc, completed), (ackId, count)) =>
            acc.acks.get(ackId) match {
              case Some(progress) =>
                val updated = progress.copy(
                  submitted = progress.submitted + count,
                  written = progress.written + count,
                )
                if (updated.shouldAck) {
                  (acc.copy(acks = acc.acks - ackId), updated :: completed)
                } else {
                  (acc.copy(acks = acc.acks.updated(ackId, updated)), completed)
                }
              case None =>
                // ackId was already removed (e.g., by a prior failBatch). The chunks did write to the DB; their
                // rows are reachable via the version_id and the next redelivery's UPSERT will overwrite them.
                (acc, completed)
            }
        }
      }
      .flatMap(completeAcks)
  }

  /**
   * For each completed ackId: if it wrote rows, trim any stale tail past its chunk count and markFetched on the version
   * row; then invoke `ack` regardless. Errors in trim or markFetched switch the outcome to NACK for that ackId —
   * Pub/Sub will redeliver, the UPSERT is idempotent, and the next attempt's trim + markFetched run again.
   *
   * Errors are isolated per-ackId so one bill's markFetched failure doesn't poison the others in the same batch.
   */
  private[embedding] def completeAcks(completed: List[AckProgress[F]]): F[Unit] =
    completed.traverse_(completeAck)

  private[embedding] def completeAck(progress: AckProgress[F]): F[Unit] = {
    val logCtx = LogContext(
      runId = progress.ackId,
      stepName = StepName,
      correlationId = None,
      entityId = Some(progress.ctx.naturalKey),
    )
    if (progress.written > 0) {
      runTrimAndMarkFetched(progress, logCtx).attempt.flatMap {
        case Right(()) => safeAck(progress, logCtx)
        case Left(error) =>
          logger
            .error(
              logCtx,
              s"Trim/markFetched failed for ${progress.ctx.naturalKey} (versionId=${progress.ctx.versionId.toString}); NACKing: ${describeError(error)}",
              Some(error),
            )
            .attempt
            .void *> progress.nack.attempt.void
      }
    } else {
      safeAck(progress, logCtx)
    }
  }

  /**
   * Run the caller-supplied `ack` effect, isolating failures. If `ack` raises (e.g., the processor wrapped
   * `publishIngestedEvent *> subscriber.acknowledge(ackId)` and publish failed), we log + invoke `nack` so Pub/Sub
   * redelivers; the next attempt will re-run trim + markFetched + the user's `ack` (idempotent at every step).
   */
  private[embedding] def safeAck(progress: AckProgress[F], logCtx: LogContext): F[Unit] =
    progress.ack.attempt.flatMap {
      case Right(()) => Async[F].unit
      case Left(error) =>
        logger
          .error(
            logCtx,
            s"ack callback raised for ${progress.ctx.naturalKey}; NACKing instead: ${describeError(error)}",
            Some(error),
          )
          .attempt
          .void *> progress.nack.attempt.void
    }

  private[embedding] def runTrimAndMarkFetched(progress: AckProgress[F], logCtx: LogContext): F[Unit] =
    Async[F].delay(Instant.now()).flatMap { now =>
      val cio = rawBillTextRepository.trimChunksPast(progress.ctx.versionId, progress.submitted) *>
        textVersionRepository.markFetched(progress.ctx.versionId, now)
      logger.debug(
        logCtx,
        s"Completing ${progress.ctx.naturalKey} versionId=${progress.ctx.versionId.toString}: trim past ${progress.submitted.toString} + markFetched",
      ) *> TransactionRunner.run(xa)(cio).void
    }

  /**
   * On batch-level error (embedding service raised, or DB raised): atomically remove every distinct ackId in the batch
   * from state, purge any of their submissions still buffered (a producer may have offered more chunks for one of these
   * ackIds while another fiber was flushing — those chunks are orphaned the moment we NACK the message), and invoke
   * `nack` for each. Logs the cause at ERROR. Errors from logging / nack itself are wrapped in `.attempt.void` so a
   * downstream subscriber failure can't crash the producer that happened to be the flusher.
   */
  private[embedding] def failBatch(batch: List[ChunkSubmission], error: Throwable): F[Unit] = {
    val ackIdsInBatch = batch.map(_.ackId).distinct
    val ackIdSet      = ackIdsInBatch.toSet
    val logCtx = LogContext(
      runId = "<batch>",
      stepName = StepName,
      correlationId = None,
      entityId = Some(s"${ackIdsInBatch.size.toString}-acks"),
    )
    val errorMessage = describeError(error)
    logger
      .error(
        logCtx,
        s"Embed/UPSERT batch failed for ${ackIdsInBatch.size.toString} ackId(s); NACKing each: $errorMessage",
        Some(error),
      )
      .attempt
      .void *>
      state
        .modify { s =>
          val (newAcks, removed) = ackIdsInBatch.foldLeft((s.acks, List.empty[AckProgress[F]])) {
            case ((acks, removed), ackId) =>
              acks.get(ackId) match {
                case Some(progress) => (acks - ackId, progress :: removed)
                case None           => (acks, removed)
              }
          }
          val newBuffer = s.buffer.filterNot(sub => ackIdSet.contains(sub.ackId))
          (s.copy(acks = newAcks, buffer = newBuffer), removed)
        }
        .flatMap(_.traverse_(_.nack.attempt.void))
  }

  /**
   * Submit-time error path used by [[submit]]'s `guaranteeCase` finalizer (chunk-stream error or fiber cancellation).
   * Atomically removes the ackId from state AND purges any of its submissions still sitting in the shared buffer
   * (chunks emitted before the error/cancel that hadn't triggered a flush yet — without this purge they'd ride along in
   * some other producer's later flush and waste embedding/DB work for an already-NACKed message). Logger / nack errors
   * are swallowed so a downstream subscriber failure can't stack on top of the original cause.
   */
  private[embedding] def cleanupOnSubmitError(ackId: String, error: Throwable): F[Unit] = {
    val logCtx = LogContext(
      runId = ackId,
      stepName = StepName,
      correlationId = None,
      entityId = None,
    )
    state
      .modify { s =>
        s.acks.get(ackId) match {
          case Some(progress) =>
            val newBuffer = s.buffer.filterNot(_.ackId == ackId)
            (s.copy(acks = s.acks - ackId, buffer = newBuffer), Some(progress))
          case None => (s, None)
        }
      }
      .flatMap {
        case Some(progress) =>
          logger
            .error(
              logCtx,
              s"submit() chunk stream failed for ${progress.ctx.naturalKey}; NACKing: ${describeError(error)}",
              Some(error),
            )
            .attempt
            .void *> progress.nack.attempt.void
        case None =>
          logger
            .warn(
              logCtx,
              s"submit() chunk stream failed but ackId $ackId already removed (NACK already fired): ${describeError(error)}",
            )
            .attempt
            .void
      }
  }

  private[embedding] def describeError(error: Throwable): String =
    Option(error.getMessage).getOrElse(error.getClass.getSimpleName)

}

object CrossBillEmbedder {

  /**
   * Allocate a [[CrossBillEmbedder]]. Resource holds no fibers — the only persistent state is a `Ref` for the shared
   * buffer + per-ackId progress map. Resource release is a no-op; in-flight ackIds will surface via NACK if their
   * submit fails, or already ACKed before release if they succeeded.
   *
   * @param batchSize
   *   the buffer threshold that triggers a producer-driven flush. Match this to `OLLAMA_EMBED_BATCH_SIZE` so flushes
   *   correspond to the GPU's preferred batch size.
   */
  def resource[F[_]: Async](
    embeddingService: EmbeddingService[F],
    rawBillTextRepository: RawBillTextRepository[ConnectionIO],
    textVersionRepository: BillTextVersionRepository[ConnectionIO],
    xa: Transactor[F],
    logger: PipelineLogger[F],
    batchSize: Int,
  ): Resource[F, CrossBillEmbedder[F]] =
    Resource.eval(Ref.of[F, EmbedderState[F]](EmbedderState.empty[F])).map { state =>
      new CrossBillEmbedder[F](
        embeddingService,
        rawBillTextRepository,
        textVersionRepository,
        xa,
        logger,
        state,
        batchSize,
      )
    }

}
