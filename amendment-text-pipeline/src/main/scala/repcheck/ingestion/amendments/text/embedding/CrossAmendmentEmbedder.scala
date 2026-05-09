package repcheck.ingestion.amendments.text.embedding

import cats.effect.{Async, Deferred, Ref, Resource}
import cats.syntax.all._

import fs2.Stream

import doobie._

import repcheck.ingestion.amendments.text.persistence.AmendmentTextChunkRepository
import repcheck.ingestion.bills.common.persistence.TransactionRunner
import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}
import repcheck.ingestion.text.embedding.{EmbeddingContextLengthExceeded, EmbeddingGenerationFailed, EmbeddingService}
import repcheck.pipeline.models.metadata.ProcessingResult
import repcheck.shared.models.congress.dos.amendment.AmendmentTextChunkDO

/**
 * Boundary between the per-amendment processor and the embedding strategy. Trait exists so unit tests of the processor
 * don't need to construct the full state machine. Mirror of
 * [[repcheck.ingestion.bills.text.embedding.BillChunkEmbedder]].
 */
trait AmendmentChunkEmbedder[F[_]] {

  /**
   * Submit an amendment's chunk stream for embedding + persistence. Returns when ALL chunks are processed (success) or
   * any flush containing one of this amendment's chunks fails (Failed). Mirror of
   * [[repcheck.ingestion.bills.text.embedding.CrossBillEmbedder.processChunks]].
   */
  def processChunks(ctx: AmendmentEmbedCtx, chunkStream: Stream[F, String]): F[ProcessingResult]

}

/**
 * Process-wide cross-amendment embedding accumulator. Mirror of [[CrossBillEmbedder]] for the amendment side.
 *
 * Multiple [[repcheck.ingestion.amendments.text.pipeline.AmendmentTextProcessor]] invocations (one per amendment,
 * running concurrently in the outer pipeline's `parEvalMap`) submit chunks here. Chunks accumulate in a shared buffer
 * until any producer's offer fills it to `batchSize`, at which point THAT PRODUCER synchronously embeds + persists the
 * entire buffer (mixed across amendments) and resolves the affected amendments' Deferreds.
 *
 * ==Foreground-only design==
 *
 * No background fiber. Every embedding + DB write happens on the producing fiber that triggered the flush. Errors
 * propagate through that fiber's normal IO channel and surface in the amendment's `processChunks` result. The earlier
 * background-fiber design hung in production for the bill-side; we copy the foreground-only fix.
 *
 * ==Per-amendment completion contract==
 *
 * `processChunks` returns an `F[ProcessingResult]` that resolves when ALL of THIS amendment's chunks have been INSERTed
 * (success) OR when any flush containing one of this amendment's chunks fails (Failed).
 *
 * ==Why cross-amendment batching matters==
 *
 * Same logic as the bill side: amendments are usually small (often a single 12k chunk for short floor amendments).
 * Without cross-amendment batching the GPU sits idle waiting for one-chunk batches. Mixing chunks across amendments
 * inside one Ollama call closes the utilization gap.
 */
class CrossAmendmentEmbedder[F[_]: Async] private[embedding] (
  embeddingService: EmbeddingService[F],
  amendmentTextChunkRepository: AmendmentTextChunkRepository[ConnectionIO],
  xa: Transactor[F],
  logger: PipelineLogger[F],
  state: Ref[F, AmendmentEmbedderState[F]],
  batchSize: Int,
) extends AmendmentChunkEmbedder[F] {

  private val StepName = "cross-amendment-embedder"

  override def processChunks(ctx: AmendmentEmbedCtx, chunkStream: Stream[F, String]): F[ProcessingResult] =
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

  private[embedding] def register(ctx: AmendmentEmbedCtx): F[F[ProcessingResult]] =
    Deferred[F, ProcessingResult].flatMap { deferred =>
      val initial = AmendmentEmbedProgress[F](
        ctx = ctx,
        expected = None,
        persisted = 0,
        deferred = deferred,
      )
      state.update(s => s.copy(amendments = s.amendments + (ctx.versionId -> initial))).as(deferred.get)
    }

  private[embedding] def offerChunk(ctx: AmendmentEmbedCtx, chunkIdx: Int, text: String): F[Unit] = {
    val submission = AmendmentChunkSubmission(ctx, chunkIdx, text)
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

  private[embedding] def finalizeSubmission(ctx: AmendmentEmbedCtx, count: Int): F[Unit] =
    state
      .modify { s =>
        s.amendments.get(ctx.versionId) match {
          case Some(progress) =>
            val updated = progress.copy(expected = Some(count))
            val flush   = s.buffer.toList
            val newAmendments =
              if (updated.shouldComplete) {
                s.amendments - ctx.versionId
              } else {
                s.amendments.updated(ctx.versionId, updated)
              }
            val maybeDone = if (updated.shouldComplete) Some(updated) else None
            (s.copy(buffer = Vector.empty, amendments = newAmendments), (maybeDone, flush))
          case None =>
            (s, (None, List.empty[AmendmentChunkSubmission]))
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

  private[embedding] def cleanupIfStillRegistered(ctx: AmendmentEmbedCtx): F[Unit] =
    state
      .modify { s =>
        s.amendments.get(ctx.versionId) match {
          case Some(progress) =>
            (s.copy(amendments = s.amendments - ctx.versionId), Some(progress))
          case None => (s, None)
        }
      }
      .flatMap {
        case Some(progress) =>
          progress.deferred
            .complete(
              ProcessingResult.Failed(
                progress.ctx.naturalKey,
                "Amendment processing aborted before submission finalized",
                "Systemic",
              )
            )
            .void
        case None => Async[F].unit
      }

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
                AmendmentTextChunkDO(
                  id = 0L,
                  amendmentId = sub.ctx.amendmentId,
                  versionId = Some(sub.ctx.versionId),
                  chunkIndex = sub.chunkIdx,
                  content = sub.text,
                  embedding = emb,
                  createdAt = None,
                )
            }
            TransactionRunner
              .run(xa)(amendmentTextChunkRepository.insertMany(rows))
              .attempt
              .flatMap {
                case Left(error) => failBatch(batch, error)
                case Right(_)    => applyBatchResult(batch)
              }
        }
    }

  private[embedding] def applyBatchResult(batch: List[AmendmentChunkSubmission]): F[Unit] = {
    val perVersionCounts: Map[Long, Int] = batch.groupMapReduce(_.ctx.versionId)(_ => 1)(_ + _)
    state
      .modify { s =>
        perVersionCounts.foldLeft((s, List.empty[AmendmentEmbedProgress[F]])) {
          case ((acc, completed), (versionId, count)) =>
            acc.amendments.get(versionId) match {
              case Some(progress) =>
                val updated = progress.copy(persisted = progress.persisted + count)
                if (updated.shouldComplete) {
                  (acc.copy(amendments = acc.amendments - versionId), updated :: completed)
                } else {
                  (acc.copy(amendments = acc.amendments.updated(versionId, updated)), completed)
                }
              case None =>
                // Version was removed (e.g. by a prior failBatch). Chunks still got embedded + INSERTed; the
                // orphan rows are cleaned up by the next pipeline tick's `clearOrphanChunks`.
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

  private[embedding] def failBatch(batch: List[AmendmentChunkSubmission], error: Throwable): F[Unit] = {
    val versionsInBatch = batch.map(_.ctx).distinctBy(_.versionId)
    val logCtx = LogContext(
      runId = "<batch>",
      stepName = StepName,
      correlationId = None,
      entityId = Some(s"${versionsInBatch.size}-versions"),
    )
    val errorMessage = Option(error.getMessage).getOrElse(error.getClass.getSimpleName)
    logger.error(
      logCtx,
      s"Embed/INSERT batch failed for ${versionsInBatch.size} version(s); failing each: $errorMessage",
      Some(error),
    ) *>
      state
        .modify { s =>
          versionsInBatch.foldLeft((s, List.empty[AmendmentEmbedProgress[F]])) {
            case ((acc, removed), ctx) =>
              acc.amendments.get(ctx.versionId) match {
                case Some(progress) =>
                  (acc.copy(amendments = acc.amendments - ctx.versionId), progress :: removed)
                case None => (acc, removed)
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

object CrossAmendmentEmbedder {

  /**
   * Allocate a [[CrossAmendmentEmbedder]]. Holds no fibers — the only persistent state is a `Ref` for the shared buffer
   * + per-amendment progress map. Resource release is effectively a no-op; in-flight amendments' processChunks calls
   * own their own Deferreds and resolve them via flushes or `cleanupIfStillRegistered`.
   *
   * @param batchSize
   *   the buffer threshold that triggers a producer-driven flush. Match this to `OLLAMA_EMBED_BATCH_SIZE`.
   */
  def resource[F[_]: Async](
    embeddingService: EmbeddingService[F],
    amendmentTextChunkRepository: AmendmentTextChunkRepository[ConnectionIO],
    xa: Transactor[F],
    logger: PipelineLogger[F],
    batchSize: Int,
  ): Resource[F, CrossAmendmentEmbedder[F]] =
    Resource.eval(Ref.of[F, AmendmentEmbedderState[F]](AmendmentEmbedderState.empty[F])).map { state =>
      new CrossAmendmentEmbedder[F](embeddingService, amendmentTextChunkRepository, xa, logger, state, batchSize)
    }

}
