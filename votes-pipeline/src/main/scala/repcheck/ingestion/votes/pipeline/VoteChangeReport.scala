package repcheck.ingestion.votes.pipeline

import difflicious.DiffResult

/**
 * Result of [[VoteChangeDetector.detect]] — tells the downstream processor how to handle the incoming vote.
 *
 * Three states drive the vote-pipeline decision matrix (§6.4 spec):
 *   - [[New]]: no stored row — insert, emit `vote.recorded` (isUpdate=false), no archive needed.
 *   - [[Updated]]: stored row with an older `updateDate` — archive current row, upsert incoming, and (iff
 *     `positionsChanged`) emit `vote.recorded` with `isUpdate=true`. Metadata-only updates (`positionsChanged = false`)
 *     still archive + upsert but skip event emission because scoring is position-driven.
 *   - [[Unchanged]]: stored row with equal-or-newer `updateDate` — no-op. Regression (older incoming) and missing
 *     `updateDate` on either side also fall into this bucket so the caller doesn't have to disambiguate.
 *
 * Mirrors the shape of [[repcheck.ingestion.common.changes.ChangeReport.Updated]]: the difflicious `DiffResult` is
 * carried forward so callers can render per-member log lines without the detector needing a bespoke ADT. Only the
 * `positionsChanged` boolean (derived from `!diff.isOk`) leaks into the contract — everything else is observability.
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
   *   `true` iff any member's position added, removed, or changed — gates `vote.recorded` event emission. Equivalent to
   *   `!diff.isOk` but hoisted onto the report so callers don't re-derive it.
   * @param diff
   *   structured difflicious diff of incoming vs stored position lists (keyed by `memberId`). Empty-item list or
   *   `isOk=true` when only vote metadata changed.
   */
  case Updated(positionsChanged: Boolean, diff: DiffResult) extends VoteChangeReport

  /**
   * Stored row's `updateDate` is already at or ahead of the incoming row — no action. Also the branch for the
   * regression case (incoming strictly older than stored) and the defensive case where either side is missing
   * `updateDate`, so the caller doesn't have to disambiguate.
   */
  case Unchanged extends VoteChangeReport

}
