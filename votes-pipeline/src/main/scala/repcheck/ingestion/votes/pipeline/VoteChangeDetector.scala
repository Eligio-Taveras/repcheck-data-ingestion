package repcheck.ingestion.votes.pipeline

import java.util.UUID

import cats.effect.Sync
import cats.syntax.all._

import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}
import repcheck.ingestion.votes.persistence.{VotePositionRepository, VoteRepository}
import repcheck.shared.models.congress.dos.vote.{VoteDO, VotePositionDO}

/**
 * Vote-specific change detection that decides whether an incoming vote is [[VoteChangeReport.New]],
 * [[VoteChangeReport.Updated]], or [[VoteChangeReport.Unchanged]] relative to stored state. Lives in votes-pipeline
 * (not ingestion-common) because the generic [[repcheck.ingestion.common.changes.ChangeDetector]] cannot express
 * "positions changed" — positions are a separate row collection, not fields on [[VoteDO]].
 *
 * Called by `VoteProcessor` (P3.1) once per incoming vote, after DTO→DO conversion and LIS resolution. The result
 * drives the §6.4 decision matrix (archive? upsert? emit event?).
 *
 * ==Key behavior==
 *
 *   - **New**: stored lookup returns `None` → [[VoteChangeReport.New]]. Positions are NEVER fetched for this branch (a
 *     new vote has none stored yet).
 *   - **Unchanged (date regression)**: incoming `updateDate` is strictly older than stored → warn (the upstream API
 *     should never regress), return [[VoteChangeReport.Unchanged]]. Positions are NEVER fetched (AC#3).
 *   - **Unchanged (same date)**: incoming `updateDate` equals stored → [[VoteChangeReport.Unchanged]]. Positions are
 *     NEVER fetched — a deliberate optimization (AC#2): if the upstream `updateDate` didn't move, the payload is
 *     assumed identical, skipping the DB round-trip.
 *   - **Updated**: incoming `updateDate` strictly newer → fetch stored positions, diff against `resolvedPositions`,
 *     emit [[VoteChangeReport.Updated]] with `positionsChanged = diffs.nonEmpty`.
 *
 * ==Position comparison==
 *
 * Positions are compared by `memberId: Long` only — see [[VotePositionDiff]] for the rationale and the placeholder→real
 * merge semantics. Order-independence is a property: `detect(voteDo, shuffle(positions), id)` returns the same report
 * as `detect(voteDo, positions, id)` for any input. Enforced by `VotePositionDiffPropSpec`.
 *
 * ==Log context==
 *
 * Every log entry carries the caller-supplied `correlationId` via [[LogContext]] so a single vote can be traced across
 * the processor, detector, repository, and event publisher.
 *
 * @param voteRepo
 *   for the natural-key lookup.
 * @param positionRepo
 *   for the `findByVoteId` call. Only invoked on the "newer updateDate + stored row exists" branch — the `never()`
 *   assertions in the unit test rely on this.
 * @param logger
 *   structured logger for info/warn entries. Diffs are logged at info, regressions at warn.
 */
class VoteChangeDetector[F[_]: Sync](
  voteRepo: VoteRepository[F],
  positionRepo: VotePositionRepository[F],
  logger: PipelineLogger[F],
) {

  private val StepName: String = "vote-change-detection"

  /**
   * Decide whether `voteDo` represents a change relative to stored state.
   *
   * See the class docstring for branch behavior. `resolvedPositions` must already have `memberId: Long` populated
   * (i.e., post-[[repcheck.ingestion.votes.pipeline.VoteChangeDetector]]'s upstream LIS resolution + placeholder
   * creation). Comparing against un-resolved rows would produce spurious `Removed(placeholder) + Added(real)` diffs on
   * every run.
   */
  def detect(
    voteDo: VoteDO,
    resolvedPositions: List[VotePositionDO],
    correlationId: UUID,
  ): F[VoteChangeReport] = {
    val logCtx = LogContext(
      runId = "detector",
      stepName = StepName,
      correlationId = Some(correlationId),
      entityId = Some(voteDo.naturalKey),
    )

    voteRepo.findByNaturalKey(voteDo.naturalKey).flatMap {
      case None           => handleNew(voteDo, logCtx)
      case Some(storedDo) => compareAgainstStored(voteDo, storedDo, resolvedPositions, logCtx)
    }
  }

  /**
   * Stored lookup returned `None`. Log once and emit [[VoteChangeReport.New]]. Deliberately does NOT touch
   * `positionRepo` — there is nothing stored to compare against.
   */
  private def handleNew(voteDo: VoteDO, logCtx: LogContext): F[VoteChangeReport] =
    logger
      .info(logCtx, s"New vote detected: ${voteDo.naturalKey}")
      .as(VoteChangeReport.New)

  /**
   * Stored row exists. Dispatch on `updateDate` comparison per §6.4:
   *   - incoming strictly older → warn + [[VoteChangeReport.Unchanged]] (regression).
   *   - incoming equals stored → info + [[VoteChangeReport.Unchanged]] (fast-path; no position fetch).
   *   - incoming strictly newer → proceed to position diff.
   *
   * `Option[Instant]` comparison: pipeline contract (P1.1 / P0.1) guarantees both sides carry `Some(updateDate)` by the
   * time we get here. If either side is `None`, treat as unchanged and log — never overwrite a stored row with a
   * payload we can't date-stamp.
   */
  private def compareAgainstStored(
    incoming: VoteDO,
    stored: VoteDO,
    resolvedPositions: List[VotePositionDO],
    logCtx: LogContext,
  ): F[VoteChangeReport] =
    (incoming.updateDate, stored.updateDate) match {
      case (Some(incomingDate), Some(storedDate)) if incomingDate.isBefore(storedDate) =>
        logger
          .warn(
            logCtx,
            s"Regression detected for ${incoming.naturalKey}: incoming updateDate $incomingDate " +
              s"is older than stored $storedDate — ignoring, not overwriting",
          )
          .as(VoteChangeReport.Unchanged)

      case (Some(incomingDate), Some(storedDate)) if !incomingDate.isAfter(storedDate) =>
        logger
          .info(logCtx, s"Vote ${incoming.naturalKey} unchanged (updateDate matches stored)")
          .as(VoteChangeReport.Unchanged)

      case (Some(_), Some(_)) =>
        diffPositions(incoming, stored, resolvedPositions, logCtx)

      case _ =>
        logger
          .warn(
            logCtx,
            s"Vote ${incoming.naturalKey} has missing updateDate (incoming=${incoming.updateDate}, " +
              s"stored=${stored.updateDate}) — treating as unchanged",
          )
          .as(VoteChangeReport.Unchanged)
    }

  /**
   * Only branch that actually fetches stored positions. Computes the diff, logs each entry at info, and returns
   * [[VoteChangeReport.Updated]] with `positionsChanged = diffs.nonEmpty`.
   */
  private def diffPositions(
    incoming: VoteDO,
    stored: VoteDO,
    resolvedPositions: List[VotePositionDO],
    logCtx: LogContext,
  ): F[VoteChangeReport] =
    for {
      storedPositions <- positionRepo.findByVoteId(stored.voteId)
      diffs = computeDiffs(resolvedPositions, storedPositions)
      _ <- logDiffs(incoming.naturalKey, diffs, logCtx)
    } yield VoteChangeReport.Updated(positionsChanged = diffs.nonEmpty, diffs = diffs)

  /**
   * Pure set-diff keyed by `memberId`. Order of the output list mirrors a canonical traversal (added by incoming-order,
   * removed by stored-order, changed by incoming-order) but downstream code must not rely on that —
   * `VotePositionDiffPropSpec` only asserts set equivalence.
   *
   * Scoped `private[pipeline]` so the property test can exercise it directly without threading a [[VoteChangeDetector]]
   * instance through the generator. The detector itself is still the canonical entry point; this is an observable seam,
   * not public API.
   */
  private[pipeline] def computeDiffs(
    incoming: List[VotePositionDO],
    stored: List[VotePositionDO],
  ): List[VotePositionDiff] = {
    val incomingById = incoming.groupMapReduce(_.memberId)(identity)((a, _) => a)
    val storedById   = stored.groupMapReduce(_.memberId)(identity)((a, _) => a)

    val added = incoming.collect {
      case p if !storedById.contains(p.memberId) =>
        VotePositionDiff.Added(p.memberId, VotePositionDiff.castLabel(p.position))
    }

    val removed = stored.collect {
      case p if !incomingById.contains(p.memberId) =>
        VotePositionDiff.Removed(p.memberId, VotePositionDiff.castLabel(p.position))
    }

    val changed = incoming.flatMap { inc =>
      storedById.get(inc.memberId).toList.collect {
        case s if s.position != inc.position =>
          VotePositionDiff.Changed(
            memberId = inc.memberId,
            from = VotePositionDiff.castLabel(s.position),
            to = VotePositionDiff.castLabel(inc.position),
          )
      }
    }

    added ++ removed ++ changed
  }

  /**
   * Emit one info log per diff when any exist — §6.4 AC#10 ("3 diffs → 3 log entries with member IDs and positions").
   * When the list is empty (metadata-only update), emit a single info so the trail still shows the detector ran and
   * classified the change.
   */
  private def logDiffs(
    naturalKey: String,
    diffs: List[VotePositionDiff],
    logCtx: LogContext,
  ): F[Unit] =
    if (diffs.isEmpty) {
      logger.info(logCtx, s"Vote $naturalKey updated with no position changes (metadata-only)")
    } else {
      logger.info(logCtx, s"Vote $naturalKey updated with ${diffs.length} position change(s)") *>
        diffs.traverse_(diff => logger.info(logCtx, renderDiff(naturalKey, diff)))
    }

  private[pipeline] def renderDiff(naturalKey: String, diff: VotePositionDiff): String =
    diff match {
      case VotePositionDiff.Added(memberId, cast) =>
        s"Vote $naturalKey: added position memberId=$memberId cast=$cast"
      case VotePositionDiff.Removed(memberId, prev) =>
        s"Vote $naturalKey: removed position memberId=$memberId previousCast=$prev"
      case VotePositionDiff.Changed(memberId, from, to) =>
        s"Vote $naturalKey: changed position memberId=$memberId from=$from to=$to"
    }

}
