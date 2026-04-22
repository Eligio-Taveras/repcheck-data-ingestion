package repcheck.ingestion.votes.repo

import doobie.ConnectionIO

import repcheck.shared.models.congress.dos.vote.VotePositionDO

/**
 * Persistence trait for the `vote_positions` table. Migration 023 replaced the old composite UNIQUE `(vote_id,
 * member_id)` with a dual-identity XOR: each row populates EITHER `member_id` (House — bioguide-resolved) OR
 * `lis_member_id` (Senate — unmapped or mapped), enforced by the DB `chk_vp_xor_identity` CHECK. Two partial UNIQUE
 * indexes (`uq_vp_vote_member`, `uq_vp_vote_lis`) replace the composite PK semantics.
 *
 * Because PostgreSQL allows only ONE conflict target per `INSERT ... ON CONFLICT`, this trait splits writes into two
 * paths:
 *
 *   - `replaceAll` — still delete-then-insert, used by the change detector when positions are overwritten en bloc. The
 *     DO's populated identity column drives the INSERT per row.
 *   - `upsert` — single-row write that dispatches on which identity column is populated; each branch targets the
 *     matching partial UNIQUE index.
 *
 * Reads are also split by chamber arm: `findByVoteAndMember` targets House rows (`member_id`), `findByVoteAndLisMember`
 * targets unresolved Senate rows (`lis_member_id`). The bulk `findByVoteId` returns every row for a vote regardless of
 * arm — callers that need to diff a full vote's positions (like the change detector) get the whole set.
 *
 * `findByMemberAndBill` is retained as House-only — scoring consumers that need the Senate arm can materialize through
 * the `vote_positions_resolved` view (added by migration 023) or add a counterpart here when a specific use case
 * demands it. We deliberately do not pre-build a Senate variant to avoid unused code in this layer.
 */
trait VotePositionRepository {
  def findByVoteId(voteId: Long): ConnectionIO[List[VotePositionDO]]

  /**
   * House-arm lookup: returns the row whose `(vote_id, member_id)` matches the supplied pair, iff `member_id IS NOT
   * NULL`. Returns `None` if no House row exists for the pair (the Senate arm for the same vote is invisible to this
   * finder).
   */
  def findByVoteAndMember(voteId: Long, memberId: Long): ConnectionIO[Option[VotePositionDO]]

  /**
   * Senate-arm lookup: returns the row whose `(vote_id, lis_member_id)` matches the supplied pair, iff `lis_member_id
   * IS NOT NULL`. Returns `None` if no Senate row exists for the pair.
   */
  def findByVoteAndLisMember(voteId: Long, lisMemberId: Long): ConnectionIO[Option[VotePositionDO]]

  /**
   * Atomically replace the set of positions for a vote. Executes `DELETE FROM vote_positions WHERE vote_id = ?` first,
   * then a batch `INSERT` for every element in `positions`. An empty list still runs the DELETE, so calling
   * `replaceAll(voteId, Nil)` is the supported way to "clear" a vote's positions without dropping the parent row. Both
   * operations compose into one `ConnectionIO`; the caller wraps in a transaction to get all-or-nothing semantics.
   *
   * Each position's `memberId` or `lisMemberId` (whichever is `Some`) is written into the matching column; the other
   * column is NULL. Positions that violate the XOR invariant (both `None` or both `Some`) cause the composed
   * `ConnectionIO` to fail with [[repcheck.ingestion.votes.errors.VotePositionIdentityInvalid]] before the INSERT is
   * attempted.
   *
   * ==Archival is the caller's responsibility==
   *
   * This method deletes without archiving — it is the caller's job to invoke [[VoteHistoryArchiver.archiveVote]] first
   * when an existing set of positions is being overwritten (i.e. the `Updated` branch of the change-detection decision
   * matrix). Composing the archive call and this `replaceAll` into a single outer `ConnectionIO` gives atomic "snapshot
   * then replace" semantics. Callers on the `New` branch (no stored vote yet) skip the archive step. P3.1's
   * `VoteProcessor` owns that orchestration — see its scaladoc for the full sequencing.
   */
  def replaceAll(voteId: Long, positions: List[VotePositionDO]): ConnectionIO[Unit]

  /**
   * Insert or update a single position, dispatching on which identity column is populated:
   *
   *   - House arm (`memberId.isDefined`): conflict target is `(vote_id, member_id) WHERE member_id IS NOT NULL` per
   *     migration 023's `uq_vp_vote_member` partial UNIQUE index.
   *   - Senate arm (`lisMemberId.isDefined`): conflict target is `(vote_id, lis_member_id) WHERE lis_member_id IS NOT
   *     NULL` per migration 023's `uq_vp_vote_lis` partial UNIQUE index.
   *
   * A DO that violates the XOR invariant (both `None` or both `Some`) raises
   * [[repcheck.ingestion.votes.errors.VotePositionIdentityInvalid]] inside the `ConnectionIO` — the caller's
   * transaction rolls back cleanly.
   *
   * ==Archival is the caller's responsibility==
   *
   * On `DO UPDATE`, the prior values for this position are silently overwritten. Callers that need the prior snapshot
   * preserved must invoke [[VoteHistoryArchiver.archiveVote]] on the owning vote before this method runs, composed into
   * the same `ConnectionIO`. P3.1's `VoteProcessor` owns that sequencing for the vote-level Updated branch; ad hoc
   * `upsert` callers are responsible for matching that discipline.
   */
  def upsert(position: VotePositionDO): ConnectionIO[Unit]

  /**
   * Returns every House-arm position cast by `memberId` on votes belonging to `billId`. Implemented as a JOIN against
   * the `votes` table so callers don't have to pre-resolve the set of vote IDs. Used by downstream stance
   * materialization to pull a member's full voting record on a bill in one query.
   *
   * Senate-arm rows (populated via `lis_member_id`) are intentionally not returned by this method — scoring consumers
   * that need them should materialize through `vote_positions_resolved` or call a Senate-specific finder when added.
   */
  def findByMemberAndBill(memberId: Long, billId: Long): ConnectionIO[List[VotePositionDO]]
}
