package repcheck.ingestion.votes.pipeline

import java.util.UUID

import cats.effect.Sync
import cats.syntax.all._

import difflicious.{DiffResult, Differ}
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
 * ==Shape==
 *
 * Mirrors [[repcheck.ingestion.common.changes.ChangeDetector]] as closely as possible given the vote-specific twist:
 *   - `stored match { None => New; Some(_) => ... }` — same first-level dispatch as `ChangeDetector`.
 *   - `isStale = storedUpdateDate.forall(s => !incomingUpdateDate.isAfter(s))` — a single boolean collapses the
 *     regression, equal-date, and missing-updateDate cases into one "treat as unchanged" branch (per user review on PR
 *     #40: "let's treat something coming in on the same date as unchanged" and "let everything else fall over to the
 *     default which treats things as unchanged"). A regression is logged at warn; everything else logs at info.
 *   - When non-stale, delegate to difflicious via [[VotePositionDiffer]] for the actual position diff. The
 *     `Differ[List[VotePositionDO]]` is keyed by `memberId` (via `.pairBy`), so order is irrelevant and the
 *     `DiffResult.isOk` boolean directly encodes `positionsChanged = !isOk`.
 *
 * ==Position comparison==
 *
 * Positions are compared by `memberId: Long` only — a placeholder→real merge done by `lis-mapping-refresher` shows up
 * on the next votes-pipeline run as `ObtainedOnly(realId) + ExpectedOnly(placeholderId)`, which is semantically
 * accurate — the FK really did change. Order-independence is a property: `detect(voteDo, shuffle(positions), id)`
 * returns the same report as `detect(voteDo, positions, id)` for any input. Enforced by `VotePositionDiffPropSpec`.
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

  // Summon once at construction — the given is resolved from VotePositionDiffer's given instances in scope via the
  // import below; the detector just needs a single typeclass reference for its `diff` call.
  import VotePositionDiffer.given
  private val positionsDiffer: Differ[List[VotePositionDO]] = summon[Differ[List[VotePositionDO]]]

  /**
   * Decide whether `voteDo` represents a change relative to stored state.
   *
   * See the class docstring for branch behavior. `resolvedPositions` must already have `memberId: Long` populated
   * (i.e., post-LIS resolution + placeholder creation). Comparing against un-resolved rows would produce spurious
   * pair-mismatches on every run.
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
   * Stored row exists. Dispatch on a single `isStale` boolean per `ChangeDetector[T]`:
   *   - stale (equal dates, incoming older, or missing `updateDate` on either side) → [[VoteChangeReport.Unchanged]]. A
   *     strict regression (`incoming < stored`) is logged at warn because the upstream API should never regress;
   *     everything else is an info-level no-op. Positions are NEVER fetched on this branch (AC#2 / AC#3 require
   *     short-circuiting before the DB round-trip).
   *   - non-stale (`incoming.updateDate.isAfter(stored.updateDate)`) → fetch stored positions, run difflicious, return
   *     [[VoteChangeReport.Updated]].
   */
  private def compareAgainstStored(
    incoming: VoteDO,
    stored: VoteDO,
    resolvedPositions: List[VotePositionDO],
    logCtx: LogContext,
  ): F[VoteChangeReport] = {
    val isStale: Boolean = (incoming.updateDate, stored.updateDate) match {
      case (Some(inc), Some(sto)) => !inc.isAfter(sto)
      case _                      => true
    }

    if (isStale) {
      logStale(incoming, stored, logCtx).as(VoteChangeReport.Unchanged)
    } else {
      diffPositions(incoming, stored, resolvedPositions, logCtx)
    }
  }

  /**
   * Emit the appropriate log entry for the "treat as unchanged" branch. Strict regression gets a `warn`; every other
   * shape (equal dates, missing `updateDate`) gets an `info`.
   */
  private def logStale(
    incoming: VoteDO,
    stored: VoteDO,
    logCtx: LogContext,
  ): F[Unit] =
    (incoming.updateDate, stored.updateDate) match {
      case (Some(inc), Some(sto)) if inc.isBefore(sto) =>
        logger.warn(
          logCtx,
          s"Regression detected for ${incoming.naturalKey}: incoming updateDate $inc " +
            s"is older than stored $sto — ignoring, not overwriting",
        )
      case (Some(_), Some(_)) =>
        logger.info(logCtx, s"Vote ${incoming.naturalKey} unchanged (updateDate matches stored)")
      case _ =>
        logger.warn(
          logCtx,
          s"Vote ${incoming.naturalKey} has missing updateDate (incoming=${incoming.updateDate}, " +
            s"stored=${stored.updateDate}) — treating as unchanged",
        )
    }

  /**
   * Only branch that actually fetches stored positions. Delegates to difflicious for the diff, logs the result, and
   * derives `positionsChanged = !diff.isOk` for the emitted [[VoteChangeReport.Updated]].
   */
  private def diffPositions(
    incoming: VoteDO,
    stored: VoteDO,
    resolvedPositions: List[VotePositionDO],
    logCtx: LogContext,
  ): F[VoteChangeReport] =
    for {
      storedPositions <- positionRepo.findByVoteId(stored.voteId)
      diff = positionsDiffer.diff(resolvedPositions, storedPositions)
      _ <- logDiffs(incoming.naturalKey, diff, logCtx)
    } yield VoteChangeReport.Updated(positionsChanged = !diff.isOk, diff = diff)

  /**
   * Emit one info log per per-member change when any exist — §6.4 AC#10 ("3 diffs → 3 log entries with member IDs and
   * positions"). Walks the difflicious `ListResult.items`, rendering the `ValueResult` pair-type as a human-readable
   * line. When the list is clean (metadata-only update), emit a single summary info so the trail still shows the
   * detector ran.
   */
  private def logDiffs(
    naturalKey: String,
    diff: DiffResult,
    logCtx: LogContext,
  ): F[Unit] = {
    val entries = changedEntries(diff)
    if (entries.isEmpty) {
      logger.info(logCtx, s"Vote $naturalKey updated with no position changes (metadata-only)")
    } else {
      logger.info(logCtx, s"Vote $naturalKey updated with ${entries.length} position change(s)") *>
        entries.traverse_(entry => logger.info(logCtx, s"Vote $naturalKey: $entry"))
    }
  }

  /**
   * Walk the list-level diff and keep only items that represent a real change (`Added` / `Removed` / `Changed`). Scoped
   * `private[pipeline]` so tests can exercise the formatting directly without threading the full detector.
   */
  private[pipeline] def changedEntries(diff: DiffResult): List[String] =
    diff match {
      case list: DiffResult.ListResult =>
        list.items.iterator.collect {
          case v: DiffResult.ValueResult.ObtainedOnly => s"added position ${v.obtained}"
          case v: DiffResult.ValueResult.ExpectedOnly => s"removed position ${v.expected}"
          case v: DiffResult.ValueResult.Both if !v.isSame =>
            s"changed position from ${v.expected} to ${v.obtained}"
        }.toList
      case _ => List.empty
    }

}
