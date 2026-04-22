package repcheck.ingestion.votes.errors

/**
 * Raised by [[repcheck.ingestion.votes.repo.VotePositionRepository]] when a
 * [[repcheck.shared.models.congress.dos.vote.VotePositionDO]] violates the dual-identity XOR invariant defined by
 * migration 023. The DB `chk_vp_xor_identity` CHECK enforces that exactly one of `member_id` / `lis_member_id` is
 * populated per row; this exception fires at the Scala layer before we ever issue the INSERT, because PostgreSQL's
 * `INSERT ... ON CONFLICT` can only target ONE conflict column per statement — so `upsert` must dispatch on which
 * identity is populated, and a DO that populates neither (or both) cannot be routed to either dispatch branch.
 *
 * Surfaces `voteId` plus the observed `(memberId, lisMemberId)` pair so operators can locate the offending payload in
 * upstream logs. Unique simple-name per plugin v0.5.0: no other RepCheck exception is named
 * `VotePositionIdentityInvalid`.
 */
final case class VotePositionIdentityInvalid(
  voteId: Long,
  memberId: Option[Long],
  lisMemberId: Option[Long],
) extends Exception(
      s"VotePositionDO for vote $voteId violates XOR identity invariant: " +
        s"memberId=$memberId, lisMemberId=$lisMemberId (exactly one must be Some)"
    )
