package repcheck.ingestion.amendments.text.embedding

import java.time.Instant

import cats.effect.{Async, Ref, Resource}
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
 *   - `submitted` — chunks the producer offered. ACK fires when `submitted == expected.get`. Independent of whether
 *     trim/markFetched ran — a no-op submission (e.g. all chunks failed to write) still ACKs because there's nothing
 *     more for retry to accomplish.
 *   - `written` — chunks for this ackId that landed in the DB. Drives trim + markFetched: only run when `written > 0`
 *     so a failed submission doesn't falsely mark the version fetched.
 *
 * Under last-writer-wins UPSERT, on the happy path every offered chunk lands so `written == submitted` and both
 * triggers fire. On UPSERT/embed failure, `written` for affected ackIds stays 0 and the embedder NACKs.
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
  ): F[Unit] =
    register(ctx, ackId, ack, nack) *>
      chunkStream.zipWithIndex
        .evalMap { case (text, idx) => offerChunk(ctx, ackId, idx.toInt, text) }
        .compile
        .count
        .flatMap(count => finalizeSubmission(ackId, count.toInt))
        .handleErrorWith { error =>
          // Producer's chunk stream raised — the embedder couldn't observe a complete submission. Fail this ackId
          // (NACK + remove from state) and re-raise so the producer's effect channel still surfaces the error.
          failAck(ackId, error) *> Async[F].raiseError[Unit](error)
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

  private[embedding] def offerChunk(
    ctx: AmendmentEmbedCtx,
    ackId: String,
    chunkIdx: Int,
    text: String,
  ): F[Unit] = {
    val submission = AmendmentChunkSubmission(ctx, chunkIdx, text, ackId)
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
            TransactionRunner
              .run(xa)(amendmentTextChunkRepository.upsertMany(rows))
              .attempt
              .flatMap {
                case Left(error)        => failBatch(batch, error)
                case Right(writtenRows) => applyBatchResult(batch, writtenRows)
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
   * If this ackId has finalized AND `submitted == expected`, run completion: trim + markFetched (gated on `written >
   * 0`), invoke `ack`, remove from state. Idempotent: a second call after completion is a no-op.
   *
   * If `markFetched`/trim raise, NACK the ackId and remove from state — the chunk UPSERT is idempotent and the
   * version-row upsert path is idempotent, so redelivery converges.
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
          val finalize: F[Unit] =
            if (progress.written <= 0) {
              // No chunks landed (empty stream, or every flush filtered/failed). ACK still fires per the contract.
              progress.ack
            } else {
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
                  case Right(_) => progress.ack
                  case Left(error) =>
                    val errMsg = Option(error.getMessage).getOrElse(error.getClass.getSimpleName)
                    logger.error(
                      ackLogCtx(progress),
                      s"trim/markFetched failed for ackId=$ackId, version=$versionId: $errMsg; NACKing",
                      Some(error),
                    ) *> progress.nack
                }
            }
          finalize
      }

  /**
   * On batch-level error (embedding service raised, or UPSERT raised): atomically remove every distinct ackId in the
   * batch from state and invoke `nack` for each. Logs the cause at ERROR. Errors from logging or `nack` itself are not
   * re-raised — the caller is one of many producers and we don't want to crash the producer that happened to be the
   * flusher.
   */
  private[embedding] def failBatch(batch: List[AmendmentChunkSubmission], error: Throwable): F[Unit] = {
    val ackIdsInBatch = batch.map(_.ackId).distinct
    val logCtx = LogContext(
      runId = "<batch>",
      stepName = StepName,
      correlationId = None,
      entityId = Some(s"${ackIdsInBatch.size}-acks"),
    )
    val errorMessage = Option(error.getMessage).getOrElse(error.getClass.getSimpleName)
    logger.error(
      logCtx,
      s"Embed/UPSERT batch failed for ${ackIdsInBatch.size} ackId(s); NACKing each: $errorMessage",
      Some(error),
    ) *>
      state
        .modify { s =>
          ackIdsInBatch.foldLeft((s, List.empty[AmendmentAckProgress[F]])) {
            case ((acc, removed), ackId) =>
              acc.acks.get(ackId) match {
                case Some(progress) => (acc.copy(acks = acc.acks - ackId), progress :: removed)
                case None           => (acc, removed)
              }
          }
        }
        .flatMap(_.traverse_(_.nack))
  }

  /**
   * Single-ackId failure path used by [[submit]]'s error-handling — same shape as [[failBatch]] but scoped to one ack.
   */
  private[embedding] def failAck(ackId: String, error: Throwable): F[Unit] =
    state
      .modify { s =>
        s.acks.get(ackId) match {
          case Some(progress) => (s.copy(acks = s.acks - ackId), Some(progress))
          case None           => (s, None)
        }
      }
      .flatMap {
        case None => Async[F].unit
        case Some(progress) =>
          val errorMessage = Option(error.getMessage).getOrElse(error.getClass.getSimpleName)
          logger.error(
            ackLogCtx(progress),
            s"Submission failed for ackId=$ackId: $errorMessage; NACKing",
            Some(error),
          ) *> progress.nack
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
