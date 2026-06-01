package repcheck.ingestion.amendments.pipeline

import repcheck.ingestion.common.logging.LogContext

/**
 * Reconciles `votes` ↔ `amendments` by the roll-call number each amendment records in its own `latest_action_text`
 * (House: "… Roll no. 150", Senate: "… Record Vote Number: 4"). Runs as a post-pass after the amendments stream drains,
 * so the action text is fresh, then matches each amendment to a vote by `(congress, chamber, roll_number)` and writes
 * the linkage onto the vote row.
 *
 * This is the forward-path counterpart to the one-shot SQL backfill: the amendments pipeline owns `latest_action_text`
 * and runs frequently, so reconciling here keeps `votes.amendment_id` (+ the natural-key columns) current as new
 * amendment votes arrive — regardless of whether the amendment or the vote was ingested first.
 *
 * En-bloc votes (one roll-call agreeing to several amendments) are deliberately skipped: a single `amendment_id` FK
 * can't represent the many-to-one relationship, so those vote rows are left unlinked rather than arbitrarily pointed at
 * one of the bloc.
 */
trait VoteAmendmentLinker[F[_]] {

  /**
   * Link every unambiguous amendment↔vote pair discoverable from `latest_action_text`. Idempotent — only rows whose
   * `amendment_id` would change are touched. Returns the number of vote rows updated this pass.
   */
  def linkAll(ctx: LogContext): F[Int]

}
