package repcheck.members.committees.model

import java.time.Instant

/**
 * Domain object for the `committee_members` table. Field order matches the explicit SELECT column list in
 * DoobieCommitteeMemberRepository (Doobie maps positionally).
 */
final case class CommitteeMemberDO(
  id: Long,
  committeeId: Long,
  memberId: Long,
  role: Option[String],
  startDate: Option[Instant],
  endDate: Option[Instant],
  side: Option[String],
  rank: Option[Int],
  congress: Int,
  createdAt: Option[Instant],
  updatedAt: Option[Instant],
)
