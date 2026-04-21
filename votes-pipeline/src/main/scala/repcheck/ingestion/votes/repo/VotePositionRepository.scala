package repcheck.ingestion.votes.repo

import doobie.ConnectionIO

import repcheck.shared.models.congress.dos.vote.VotePositionDO

/**
 * Persistence trait for the `vote_positions` table. Positions live under a composite uniqueness constraint `(vote_id,
 * member_id)` (migration 011 `uq_vote_positions`), so writes flow through `replaceAll`: all rows for a vote are deleted
 * and replaced in a single atomic `ConnectionIO`, avoiding the need to diff individual positions.
 *
 * The upstream `VoteChangeDetector` handles position diffing for event-emission logic — the repository itself is
 * deliberately dumb about which positions changed.
 */
trait VotePositionRepository {
  def findByVoteId(voteId: Long): ConnectionIO[List[VotePositionDO]]

  /**
   * Atomically replace the set of positions for a vote. Executes `DELETE FROM vote_positions WHERE vote_id = ?` first,
   * then a batch `INSERT` for every element in `positions`. An empty list still runs the DELETE, so calling
   * `replaceAll(voteId, Nil)` is the supported way to "clear" a vote's positions without dropping the parent row. Both
   * operations compose into one `ConnectionIO`; the caller wraps in a transaction to get all-or-nothing semantics.
   */
  def replaceAll(voteId: Long, positions: List[VotePositionDO]): ConnectionIO[Unit]

  /**
   * Returns every position cast by `memberId` on votes belonging to `billId`. Implemented as a JOIN against the `votes`
   * table so callers don't have to pre-resolve the set of vote IDs. Used by downstream stance materialization to pull a
   * member's full voting record on a bill in one query.
   */
  def findByMemberAndBill(memberId: Long, billId: Long): ConnectionIO[List[VotePositionDO]]
}
