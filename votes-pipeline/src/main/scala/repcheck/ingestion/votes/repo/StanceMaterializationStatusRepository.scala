package repcheck.ingestion.votes.repo

import doobie.ConnectionIO

/**
 * Persistence trait for the `stance_materialization_status` table. This table is the shared handoff between the votes
 * pipeline (which sets `has_votes = TRUE` once votes land for a bill) and the stance materialization scanner (which
 * polls for bills that have both votes and completed analysis). The votes pipeline only touches two columns —
 * `has_votes` and `votes_updated_at` — so the repository exposes a single `markHasVotes` method.
 */
trait StanceMaterializationStatusRepository {

  /**
   * Flip `has_votes` to TRUE for the given `billId` and refresh `votes_updated_at` to NOW(). Upserts the row: if the
   * bill already has a tracker row (from a prior pipeline pass or the analysis pipeline), it is updated in place;
   * otherwise a new row is created with `has_votes = TRUE` and other flags defaulting to FALSE. Idempotent.
   */
  def markHasVotes(billId: Long): ConnectionIO[Unit]
}
