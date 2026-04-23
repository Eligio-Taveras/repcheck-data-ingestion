package repcheck.ingestion.votes.pipeline

import java.time.ZoneOffset
import java.util.UUID

import cats.effect.Async
import cats.syntax.all._

import doobie.implicits._
import doobie.util.transactor.Transactor

import repcheck.ingestion.common.events.IngestionEventPublisher
import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}
import repcheck.ingestion.votes.errors.VoteProcessingFailed
import repcheck.ingestion.votes.repo.StanceMaterializationStatusRepository
import repcheck.pipeline.models.events.VoteRecordedEvent
import repcheck.shared.models.congress.dos.vote.VoteDO

/**
 * Side-effectful emission for the happy path. When a vote is `New` or `Updated(positionsChanged = true)` the processor
 * calls [[emitSuccess]], which runs two steps in order:
 *   1. If the persisted vote is bill-linked, upsert `stance_materialization_status.has_votes = true`. Procedural votes
 *      skip this call because the stance table's primary key is `bill_id` — there is no row shape for a non-bill vote.
 *      2. Publish a [[VoteRecordedEvent]] with the resolved natural keys, chamber, date, and `isUpdate` flag. Publish
 *      failures are wrapped in [[VoteProcessingFailed]] so per-vote failure isolation (`handleErrorWith` inside
 *      `parEvalMap`) records `ProcessingResult.Failed(voteNaturalKey, ...)` without aborting the chamber stream.
 */
class VoteEventEmitter[F[_]: Async](
  stanceRepo: StanceMaterializationStatusRepository,
  eventPublisher: IngestionEventPublisher[F],
  xa: Transactor[F],
  logger: PipelineLogger[F],
) {

  def emitSuccess(
    persisted: VoteDO,
    billNaturalKey: Option[String],
    isUpdate: Boolean,
    correlationId: UUID,
    logCtx: LogContext,
  ): F[Unit] =
    maybeMarkStance(persisted.billId, logCtx) *>
      emitEvent(persisted, billNaturalKey, isUpdate, correlationId, logCtx)

  private def maybeMarkStance(billId: Option[Long], logCtx: LogContext): F[Unit] =
    billId match {
      case None => Async[F].unit
      case Some(b) =>
        stanceRepo.markHasVotes(b).transact(xa).void *>
          logger.info(logCtx, s"Marked stance_materialization_status.has_votes=true for bill_id=${b.toString}")
    }

  private def emitEvent(
    persisted: VoteDO,
    billNaturalKey: Option[String],
    isUpdate: Boolean,
    correlationId: UUID,
    logCtx: LogContext,
  ): F[Unit] = {
    val dateInstant = persisted.updateDate
      .orElse(persisted.voteDate.map(_.atStartOfDay().toInstant(ZoneOffset.UTC)))
      .getOrElse(java.time.Instant.EPOCH)
    val event = VoteRecordedEvent(
      voteNaturalKey = persisted.naturalKey,
      billNaturalKey = billNaturalKey,
      chamber = persisted.chamber.apiValue,
      date = dateInstant,
      congress = persisted.congress,
      isUpdate = isUpdate,
    )
    eventPublisher
      .voteRecorded(event, correlationId)
      .flatMap(msgId =>
        logger.info(
          logCtx,
          s"Published VoteRecordedEvent for ${persisted.naturalKey} (isUpdate=${isUpdate.toString}) as $msgId",
        )
      )
      .handleErrorWith { e =>
        logger.error(
          logCtx,
          s"Failed to publish VoteRecordedEvent for ${persisted.naturalKey}: ${e.getMessage}",
          Some(e),
        ) *>
          Async[F].raiseError[Unit](
            VoteProcessingFailed(persisted.naturalKey, s"event publish failed: ${e.getMessage}", Some(e))
          )
      }
  }

}
