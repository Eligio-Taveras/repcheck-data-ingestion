package repcheck.ingestion.votes.pipeline

/**
 * Result of [[VoteChangeDetector.detect]] — tells the downstream processor how to handle the incoming vote.
 *
 * Three states drive the vote-pipeline decision matrix (§6.4 spec):
 *   - [[New]]: no stored row — insert, emit `vote.recorded` (isUpdate=false), no archive needed.
 *   - [[Updated]]: stored row with an older `updateDate` — archive current row, upsert incoming, and (iff
 *     `positionsChanged`) emit `vote.recorded` with `isUpdate=true`. Metadata-only updates (`positionsChanged = false`)
 *     still archive + upsert but skip event emission because scoring is position-driven.
 *   - [[Unchanged]]: stored row with equal-or-newer `updateDate` — no-op. Regression (older incoming) is logged as warn
 *     and falls into this bucket; equal-dates silently fall through without a second DB round-trip.
 *
 * This ADT intentionally carries no [[repcheck.shared.models.congress.dos.results.VoteConversionResult]] or
 * [[repcheck.shared.models.congress.dos.vote.VoteDO]] reference: the processor already has the incoming payload when it
 * calls `detect`, so threading it back through the report would double-count without adding signal. Diffs are surfaced
 * for logging / observability; callers do not persist them (plan §6.4 rationale — only the final position set is
 * stored).
 */
enum VoteChangeReport {

  /**
   * No stored row for this natural key — this is the first time we have seen this vote.
   */
  case New extends VoteChangeReport

  /**
   * Stored row exists and incoming strictly supersedes it by `updateDate`.
   *
   * @param positionsChanged
   *   `true` iff any member's cast added, removed, or changed — gates `vote.recorded` event emission.
   * @param diffs
   *   per-member difference list for logging / observability. Empty when `positionsChanged = false`.
   */
  case Updated(positionsChanged: Boolean, diffs: List[VotePositionDiff]) extends VoteChangeReport

  /**
   * Stored row's `updateDate` is already at or ahead of the incoming row — no action. Also the branch for the
   * regression case (incoming strictly older than stored) so the caller doesn't have to disambiguate.
   */
  case Unchanged extends VoteChangeReport

}
