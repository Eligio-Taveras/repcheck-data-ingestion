package repcheck.ingestion.amendments.text.embedding

import java.time.Instant

import cats.effect.{Async, Outcome, Ref, Resource}
import cats.syntax.all._

import fs2.Stream

import doobie._

import repcheck.ingestion.amendments.text.persistence.{
  AmendmentChunkRow,
  AmendmentTextChunkRepository,
  AmendmentTextVersionRepository,
}
import repcheck.ingestion.bills.common.persistence.TransactionRunner
import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}
import repcheck.ingestion.text.embedding.EmbeddingService

/**
 * Boundary between the per-amendment processor and the embedding strategy. The trait exists so unit tests of the
 * processor don't need to construct the full state machine. Mirror of the bill-side `BillChunkEmbedder`.
 *
 * ==Submit-and-return contract==
 *
 * Unlike the previous `processChunks: F[ProcessingResult]` shape, [[submit]] is fire-and-forget. The producer (the
 * processor) hands the embedder the chunk stream + the Pub/Sub ack/nack effects and returns immediately. The embedder
 * owns the rest: chunk buffering, embed + UPSERT, trim past the new submission's tail, mark the version fetched, and
 * finally invoke `ack`. On any known failure the embedder invokes `nack` so Pub/Sub redelivers (bounded by the
 * subscription's `max_delivery_attempts` + dead-letter topic).
 */
trait AmendmentChunkEmbedder[F[_]] {

  /**
   * Submit an amendment's chunk stream for embedding + persistence. Returns when chunks are enqueued and the residual
   * flush is driven; the actual ACK fires asynchronously when the last of this `ackId`'s chunks lands. Mirror of
   * `CrossBillEmbedder.submit`.
   *
   * @param ctx
   *   amendment surrogate ids + natural key for log / error attribution.
   * @param chunkStream
   *   the per-amendment chunk producer (extractor → chunker output). Each emitted `String` becomes one chunk
   *   submission. An empty stream is valid and triggers an immediate ACK with `written = 0`.
   * @param ackId
   *   the Pub/Sub ack identifier for the originating `amendment.text.available` message. ACK / NACK both reference it.
   * @param ack
   *   Pub/Sub acknowledge effect. Invoked exactly once when this ackId's `submitted == expected`.
   * @param nack
   *   Pub/Sub explicit-redeliver effect. Invoked on known failures; the chunk UPSERT + trim are idempotent so retries
   *   converge.
   */
  def submit(
    ctx: AmendmentEmbedCtx,
    chunkStream: Stream[F, String],
    ackId: String,
    ack: F[Unit],
    nack: F[Unit],
  ): F[Unit]

}

/**
 * Process-wide cross-amendment embedding accumulator. Mirror of `CrossBillEmbedder` for the amendment side.
 *
 * Multiple [[repcheck.ingestion.amendments.text.pipeline.AmendmentTextProcessor]] invocations (one per amendment,
 * running concurrently in the outer pipeline's `parEvalMap`) submit chunks here. Chunks accumulate in a shared buffer
 * until any producer's offer fills it to `batchSize`, at which point THAT PRODUCER synchronously embeds + UPSERTs the
 * entire buffer (mixed across amendments + ackIds) and updates the affected ackIds' counters.
 *
 * ==Foreground-only design==
 *
 * No background fiber. Every embedding + DB write happens on the producing fiber that triggered the flush. Errors
 * propagate through that fiber's normal IO channel and surface as `nack` on the affected ackIds.
 *
 * ==Per-ackId completion==
 *
 * Two counters track each ackId's progress:
 *
 *   - `submitted` — chunks for this ackId that have been embedded + persisted in a successful batch (incremented in
 *     `applyBatchResult`, never at offer time). ACK fires when `submitted == expected.get`.
 *   - `written` — of those, the affected-row count from `upsertMany`. Drives trim + markFetched: only run when `written
 *     > 0` so a no-op or failed-then-cleared submission doesn't falsely mark the version fetched.
 *
 * Under last-writer-wins UPSERT, on the happy path every offered chunk lands so `written == submitted` and both
 * triggers fire. On embed/UPSERT failure, `failBatch` removes the affected ackIds AND purges their buffered chunks from
 * the shared buffer, then NACKs each. On stream error or fiber cancellation, `submit`'s `guaranteeCase` finalizer
 * routes through `failAck` for the same cleanup + NACK.
 *
 * ==Why cross-amendment batching matters==
 *
 * Same logic as the bill side: amendments are usually small (often a single 12k chunk for short floor amendments).
 * Without cross-amendment batching the GPU sits idle waiting for one-chunk batches. Mixing chunks across amendments +
 * ackIds inside one Ollama call closes the utilization gap.
 */
class CrossAmendmentEmbedder[F[_]: Async] private[embedding] (
  embeddingService: EmbeddingService[F],
  amendmentTextChunkRepository: AmendmentTextChunkRepository[ConnectionIO],
  amendmentTextVersionRepository: AmendmentTextVersionRepository[ConnectionIO],
  xa: Transactor[F],
  logger: PipelineLogger[F],
  state: Ref[F, AmendmentEmbedderState[F]],
  batchSize: Int,
) extends AmendmentChunkEmbedder[F] {

  private val StepName = "cross-amendment-embedder"

  override def submit(
    ctx: AmendmentEmbedCtx,
    chunkStream: Stream[F, String],
    ackId: String,
    ack: F[Unit],
    nack: F[Unit],
  ): F[Unit] = {
    val producer: F[Unit] =
      register(ctx, ackId, ack, nack) *>
        chunkStream.zipWithIndex
          .evalMap { case (text, idx) => offerChunk(ctx, ackId, idx.toInt, text) }
          .compile
          .count
          .flatMap(count => finalizeSubmission(ackId, count.toInt))

    // `guaranteeCase` covers all three exit paths so the ackId never leaks in the state map:
    //   - Errored: producer's chunk stream raised — purge buffered chunks + NACK + re-raise (Async re-raises).
    //   - Canceled: fiber cancelled (shutdown / supervisor cancel) — purge buffered chunks + NACK so Pub/Sub
    //     redelivers; otherwise the entry would sit in `acks` indefinitely and chunks would orphan in the buffer.
    //   - Succeeded: no cleanup here — completion fires via `applyBatchResult`/`finalizeSubmission` paths.
    Async[F].guaranteeCase(producer) {
      case Outcome.Succeeded(_) => Async[F].unit
      case Outcome.Errored(e)   => failAck(ackId, e)
      case Outcome.Canceled()   => failAck(ackId, AmendmentSubmissionCancelled(ackId))
    }
  }

  private[embedding] def register(
    ctx: AmendmentEmbedCtx,
    ackId: String,
    ack: F[Unit],
    nack: F[Unit],
  ): F[Unit] = {
    val initial = AmendmentAckProgress[F](
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
   * Append one chunk to the shared buffer iff the ackId is still registered. If the ackId was already removed (a
   * concurrent `failBatch`/`failAck` ran between two of this producer's offers, e.g. an earlier batch's flush errored),
   * drop the chunk on the floor — the ackId has already been NACKed, so persisting more rows for it would orphan them.
   * The producer's own chunk stream is the only thing that could re-introduce them and it will exit via `submit`'s
   * `guaranteeCase` finalizer; no special signaling needed.
   */
  private[embedding] def offerChunk(
    ctx: AmendmentEmbedCtx,
    ackId: String,
    chunkIdx: Int,
    text: String,
  ): F[Unit] = {
    val submission = AmendmentChunkSubmission(ctx, chunkIdx, text, ackId)
    state
      .modify { s =>
        if (!s.acks.contains(ackId)) {
          (s, List.empty[AmendmentChunkSubmission])
        } else {
          val newBuffer = s.buffer :+ submission
          if (newBuffer.size >= batchSize) {
            (s.copy(buffer = Vector.empty), newBuffer.toList)
          } else {
            (s.copy(buffer = newBuffer), List.empty[AmendmentChunkSubmission])
          }
        }
      }
      .flatMap(flushIfNonEmpty)
  }

  /**
   * Set `expected` for an ackId AND atomically take the residual buffer for force-flushing. After the residual flush
   * runs, re-check whether this ackId completed (its chunks may already have been persisted by an earlier batch-size
   * flush before finalize was called).
   *
   * Empty-stream (`count == 0`): `expected = Some(0)`, `submitted = 0` → `shouldComplete` true → ACK fires immediately
   * without trim/markFetched (`written == 0`).
   */
  private[embedding] def finalizeSubmission(ackId: String, count: Int): F[Unit] =
    state
      .modify { s =>
        s.acks.get(ackId) match {
          case Some(progress) =>
            val updated = progress.copy(expected = Some(count))
            val flush   = s.buffer.toList
            (s.copy(buffer = Vector.empty, acks = s.acks.updated(ackId, updated)), flush)
          case None =>
            (s, List.empty[AmendmentChunkSubmission])
        }
      }
      .flatMap(residual => flushIfNonEmpty(residual)) *> completeAckIfReady(ackId)

  /**
   * Embed + UPSERT a batch in a single foreground step, then atomically update per-ackId counters and complete any
   * fully-finished ackIds.
   *
   * Errors raised by either `embeddingService.generateEmbeddings` or `upsertMany` propagate through `attempt`. On
   * error: every distinct ackId in the failed batch has `nack` invoked and is removed from state. The error itself is
   * NOT re-raised — the calling fiber (a producer) shouldn't fail just because some other ackId's flush errored.
   *
   * ==Conflict-key dedup before UPSERT==
   *
   * Two concurrent Pub/Sub messages for the same `versionId` (different ackIds) emit chunks at the same `chunkIdx`
   * positions. If both make it into the same batch, `INSERT ... ON CONFLICT (version_id, chunk_index) DO UPDATE` would
   * raise `cannot affect row a second time` and NACK every ackId in the batch. Before calling `upsertMany`, dedup the
   * row list by `(versionId, chunkIndex)` keeping the last occurrence (last-writer-wins, deterministic). Per-ackId
   * attribution (in [[applyBatchResult]]) still uses the FULL batch — both ackIds get credit for the chunks they
   * offered because the persisted data is identical (same source bytes → same chunks for a deterministic chunker).
   */
  private[embedding] def flushIfNonEmpty(batch: List[AmendmentChunkSubmission]): F[Unit] =
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
                AmendmentChunkRow(
                  amendmentId = sub.ctx.amendmentId,
                  versionId = sub.ctx.versionId,
                  chunkIndex = sub.chunkIdx,
                  content = sub.text,
                  embedding = emb,
                )
            }
            // Last-writer-wins dedup keyed by (versionId, chunkIndex). Map fold preserves insertion order so the
            // last submission for each key in `batch` wins. Avoids both `IterableOps#last` (forbidden by WartRemover)
            // and the PostgreSQL "cannot affect row a second time" error on duplicate conflict keys.
            val deduped = rows
              .foldLeft(Map.empty[(Long, Int), AmendmentChunkRow]) { (acc, r) =>
                acc + ((r.versionId, r.chunkIndex) -> r)
              }
              .values
              .toList
            TransactionRunner
              .run(xa)(amendmentTextChunkRepository.upsertMany(deduped))
              .attempt
              .flatMap {
                case Left(error) => failBatch(batch, error)
                // applyBatchResult uses FULL batch for per-ackId attribution. We pass `batch.size` rather than the
                // deduped repo return value because every offered chunk's ackId should be credited — the dedup
                // collapses identical writes for unrelated ackIds covering the same (versionId, chunkIndex), but
                // both ackIds' submissions are conceptually "written" since the data they wrote is in the DB.
                case Right(_) => applyBatchResult(batch, batch.size)
              }
        }
    }

  /**
   * Atomically increment `submitted` and `written` for each ackId in the batch, then fire `completeAckIfReady` for
   * each. Under last-writer-wins UPSERT every offered row lands (INSERT or UPDATE) so on the happy path `writtenRows ==
   * batch.size` and each ackId's `written` increments by its full submitted count. A partial-write outcome
   * (`writtenRows < batch.size`) shouldn't happen — `upsertMany` either succeeds for the whole batch or raises (caught
   * upstream in [[flushIfNonEmpty]]'s `Left(error)` branch). If it ever does, we conservatively attribute zero
   * `written` to all ackIds so the trim/markFetched gate stays closed; ACK still fires because `submitted` is
   * independent.
   */
  private[embedding] def applyBatchResult(batch: List[AmendmentChunkSubmission], writtenRows: Int): F[Unit] = {
    val perAckCounts: Map[String, Int] = batch.groupMapReduce(_.ackId)(_ => 1)(_ + _)
    val fullyWritten: Boolean          = writtenRows >= batch.size

    state
      .modify { s =>
        val updatedAcks = perAckCounts.foldLeft(s.acks) {
          case (acks, (ackId, submittedDelta)) =>
            acks.get(ackId) match {
              case Some(progress) =>
                val writtenDelta = if (fullyWritten) submittedDelta else 0
                acks.updated(
                  ackId,
                  progress.copy(
                    submitted = progress.submitted + submittedDelta,
                    written = progress.written + writtenDelta,
                  ),
                )
              case None => acks
            }
        }
        (s.copy(acks = updatedAcks), perAckCounts.keys.toList)
      }
      .flatMap(_.traverse_(completeAckIfReady))
  }

  /**
   * If this ackId has finalized AND `submitted == expected`, run completion: trim past the new tail + markFetched
   * (always, regardless of `written` — empty submissions still record the fact that we processed the version with
   * `text_length = 0`), then invoke `ack`. Idempotent: a second call after completion is a no-op.
   *
   * Empty-stream completion behavior: `submitted = 0` → `trimChunksPast(versionId, 0)` wipes any prior chunks for that
   * versionId (LWW-consistent: the latest submission decided "no text"), and `markFetched(versionId, NOW(), 0)` records
   * the fact in the version row. Without this, `version_row.fetched_at` would stay NULL forever for empty extractions
   * even though Pub/Sub ACKed the message.
   *
   * Errors at trim/markFetched: NACK + log; the chunk UPSERT is idempotent so redelivery converges. ACK / NACK errors
   * are isolated via `safeAck` so one ackId's downstream failure can't short-circuit completion for sibling ackIds in
   * the same batch.
   */
  private[embedding] def completeAckIfReady(ackId: String): F[Unit] =
    state
      .modify { s =>
        s.acks.get(ackId) match {
          case Some(progress) if progress.shouldComplete =>
            (s.copy(acks = s.acks - ackId), Some(progress))
          case _ => (s, None)
        }
      }
      .flatMap {
        case None => Async[F].unit
        case Some(progress) =>
          val versionId = progress.ctx.versionId
          val markVersionWork: F[Unit] = Async[F].delay(Instant.now()).flatMap { now =>
            TransactionRunner.run(xa) {
              for {
                _          <- amendmentTextChunkRepository.trimChunksPast(versionId, progress.submitted)
                totalChars <- amendmentTextChunkRepository.sumContentLengthByVersionId(versionId)
                clamped = math.min(totalChars, Int.MaxValue.toLong).toInt
                _ <- amendmentTextVersionRepository.markFetched(versionId, now, clamped)
              } yield ()
            }
          }
          markVersionWork.attempt
            .flatMap {
              case Right(_) => safeAck(progress)
              case Left(error) =>
                logger
                  .error(
                    ackLogCtx(progress),
                    s"trim/markFetched failed for ackId=$ackId, version=$versionId: ${describeError(error)}; NACKing",
                    Some(error),
                  )
                  .attempt
                  .void *> progress.nack.attempt.void
            }
      }

  /**
   * Run the user-supplied `ack` effect, isolating failures. If `ack` raises (e.g., the processor wired
   * `publishIngestedEvent *> subscriber.acknowledge(ackId)` and publish failed), we log + invoke `nack` so Pub/Sub
   * redelivers; the next attempt's UPSERT + trim + markFetched + ack are all idempotent. Errors from the fallback
   * `nack` are also swallowed so one failed completion never escapes into the flushing producer's effect channel and
   * short-circuits a `traverse_(completeAckIfReady)` over sibling ackIds in the same batch.
   */
  private[embedding] def safeAck(progress: AmendmentAckProgress[F]): F[Unit] =
    progress.ack.attempt.flatMap {
      case Right(_) => Async[F].unit
      case Left(error) =>
        logger
          .error(
            ackLogCtx(progress),
            s"ack callback raised for ackId=${progress.ackId}; falling back to NACK: ${describeError(error)}",
            Some(error),
          )
          .attempt
          .void *> progress.nack.attempt.void
    }

  private[embedding] def describeError(error: Throwable): String =
    Option(error.getMessage).getOrElse(error.getClass.getSimpleName)

  /**
   * On batch-level error (embedding service raised, or UPSERT raised): atomically remove every distinct ackId in the
   * batch from state, purge any of their submissions still buffered (a later batch belonging to the same ackId may have
   * been queued since `applyBatchResult` runs only on the flushing fiber), and invoke `nack` for each. Logs the cause
   * at ERROR. Errors from logging or `nack` itself are not re-raised — the caller is one of many producers and we don't
   * want to crash the producer that happened to be the flusher.
   */
  private[embedding] def failBatch(batch: List[AmendmentChunkSubmission], error: Throwable): F[Unit] = {
    val ackIdsInBatch = batch.map(_.ackId).distinct
    val ackIdSet      = ackIdsInBatch.toSet
    val logCtx = LogContext(
      runId = "<batch>",
      stepName = StepName,
      correlationId = None,
      entityId = Some(s"${ackIdsInBatch.size}-acks"),
    )
    val errorMessage = Option(error.getMessage).getOrElse(error.getClass.getSimpleName)
    logger
      .error(
        logCtx,
        s"Embed/UPSERT batch failed for ${ackIdsInBatch.size} ackId(s); NACKing each: $errorMessage",
        Some(error),
      )
      .attempt
      .void *>
      state
        .modify { s =>
          val (newAcks, removed) = ackIdsInBatch.foldLeft((s.acks, List.empty[AmendmentAckProgress[F]])) {
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
   * Single-ackId failure path used by [[submit]]'s `guaranteeCase` finalizer (stream error or fiber cancellation) —
   * same shape as [[failBatch]] but scoped to one ack. Atomically removes the ackId from `acks` AND purges any of its
   * submissions still sitting in the shared buffer (chunks emitted by the producer before the error/cancel that hadn't
   * triggered a flush yet). Logger / nack errors are swallowed so a downstream subscriber failure can't stack on top of
   * the original cause.
   */
  private[embedding] def failAck(ackId: String, error: Throwable): F[Unit] =
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
        case None => Async[F].unit
        case Some(progress) =>
          val errorMessage = Option(error.getMessage).getOrElse(error.getClass.getSimpleName)
          logger
            .error(
              ackLogCtx(progress),
              s"Submission failed for ackId=$ackId: $errorMessage; NACKing",
              Some(error),
            )
            .attempt
            .void *> progress.nack.attempt.void
      }

  private def ackLogCtx(progress: AmendmentAckProgress[F]): LogContext =
    LogContext(
      runId = "<embedder>",
      stepName = StepName,
      correlationId = None,
      entityId = Some(progress.ctx.naturalKey),
    )

}

object CrossAmendmentEmbedder {

  /**
   * Allocate a [[CrossAmendmentEmbedder]]. Holds no fibers — the only persistent state is a `Ref` for the shared buffer
   * + per-ackId progress map. Resource release is a no-op; in-flight ackIds' state entries resolve via the shared
   * chunk-buffer flushes (`applyBatchResult`) or via the per-ackId `finalizeSubmission` `completeAckIfReady` step.
   * Failures route through [[failBatch]] (batch-level) or [[failAck]] (single-ack stream error).
   *
   * @param batchSize
   *   the buffer threshold that triggers a producer-driven flush. Match this to `OLLAMA_EMBED_BATCH_SIZE`.
   */
  def resource[F[_]: Async](
    embeddingService: EmbeddingService[F],
    amendmentTextChunkRepository: AmendmentTextChunkRepository[ConnectionIO],
    amendmentTextVersionRepository: AmendmentTextVersionRepository[ConnectionIO],
    xa: Transactor[F],
    logger: PipelineLogger[F],
    batchSize: Int,
  ): Resource[F, CrossAmendmentEmbedder[F]] =
    Resource.eval(Ref.of[F, AmendmentEmbedderState[F]](AmendmentEmbedderState.empty[F])).map { state =>
      new CrossAmendmentEmbedder[F](
        embeddingService,
        amendmentTextChunkRepository,
        amendmentTextVersionRepository,
        xa,
        logger,
        state,
        batchSize,
      )
    }

}
