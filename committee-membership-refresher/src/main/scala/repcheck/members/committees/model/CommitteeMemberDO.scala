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

object CommitteeMemberDO {

  private val ValidRoles: Set[String] =
    Set("Chairman", "Ranking Member", "Vice Chairman", "Member")

  def normalizeRole(raw: Option[String]): Option[String] =
    raw.map(_.trim).filter(ValidRoles.contains)

  def forInsert(
    committeeId: Long,
    memberId: Long,
    role: Option[String],
    side: Option[String],
    rank: Option[Int],
    congress: Int,
  ): CommitteeMemberDO =
    CommitteeMemberDO(
      id = 0L,
      committeeId = committeeId,
      memberId = memberId,
      role = normalizeRole(role),
      startDate = None,
      endDate = None,
      side = side,
      rank = rank,
      congress = congress,
      createdAt = None,
      updatedAt = None,
    )

}
