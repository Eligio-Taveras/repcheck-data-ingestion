package repcheck.ingestion.votes.persistence

import repcheck.shared.models.congress.dos.vote.VotePositionDO

/**
 * Contract for reading and writing [[VotePositionDO]] rows in AlloyDB.
 *
 * Defined in this P2.5 branch as the minimum surface P2.5 ([[repcheck.ingestion.votes.pipeline.VoteChangeDetector]])
 * requires. The canonical implementation + any additional methods (e.g. replaceAll, findByMemberAndBill) lands with
 * P2.4 (`feat/votes-repositories`). Both branches target the same trait; when P2.4 merges, this trait stays stable so
 * P2.5's callers do not change — P2.4 only grows the surface.
 *
 * Lookup is by the DB-assigned `vote_id: Long` (BIGSERIAL PK) because position rows live keyed by that FK — see
 * migration 011 / shared-models [[repcheck.shared.models.congress.dos.vote.VotePositionDO]].
 */
trait VotePositionRepository[F[_]] {

  /**
   * Fetch all stored position rows for the given internal `voteId`. Order is not guaranteed —
   * [[repcheck.ingestion.votes.pipeline.VoteChangeDetector]] treats position lists as sets and diffs them by
   * `memberId`.
   */
  def findByVoteId(voteId: Long): F[List[VotePositionDO]]

}
