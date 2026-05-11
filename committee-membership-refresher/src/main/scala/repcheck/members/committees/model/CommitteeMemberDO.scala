package repcheck.members.committees.model

import java.time.Instant

/**
 * Domain object for the `committee_members` table. Tracks temporal membership: `endDate = None` means the member is
 * currently serving on this committee.
 */
final case class CommitteeMemberDO(
  id: Long,
  committeeCode: String,
  memberId: Long,
  position: Option[String],
  side: Option[String],
  rank: Option[Int],
  congress: Int,
  beginDate: Option[Instant],
  endDate: Option[Instant],
  createdAt: Option[Instant],
  updatedAt: Option[Instant],
)
