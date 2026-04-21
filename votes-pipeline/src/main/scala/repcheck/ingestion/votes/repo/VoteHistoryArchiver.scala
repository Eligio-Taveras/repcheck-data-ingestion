package repcheck.ingestion.votes.repo

import doobie.ConnectionIO

/**
 * Archive-before-overwrite pattern for votes: snapshots the current `votes` row plus every `vote_positions` row into
 * `vote_history` and `vote_history_positions` BEFORE the caller's upsert writes new data. The live `votes` table always
 * holds the latest version; the history tables preserve each prior revision keyed by a shared `history_id`.
 *
 * The caller is responsible for sequencing `archiveVote` BEFORE `VoteRepository.upsert` in the same transaction; the
 * archiver deliberately does not modify the live row. Returns `ConnectionIO[Long]` rather than `F[Unit]` so the caller
 * can correlate the archive event with the specific `vote_history.id` that was produced (useful for logs and downstream
 * event emission).
 *
 * =Schema note (decision deviating from plan text)=
 * Migration 011 collapsed `vote_history.history_id UUID` into `vote_history.id BIGSERIAL`; the plan and §6.3 AC were
 * written against the pre-011 schema and still reference "UUID". We follow the schema reality and return a `Long` (the
 * BIGSERIAL id). `vote_history_positions.history_id BIGINT` references the same column — the "shared history id"
 * guarantee still holds, the type is just a BIGINT instead of a UUID.
 */
trait VoteHistoryArchiver {

  /**
   * Copy the current `votes` row with primary key `voteId` into `vote_history`, and every `vote_positions` row for that
   * vote into `vote_history_positions`. Both rows share the BIGSERIAL `history_id` assigned to the new `vote_history`
   * row (the method's return value). No-op when the `voteId` does not exist.
   */
  def archiveVote(voteId: Long): ConnectionIO[Long]
}
