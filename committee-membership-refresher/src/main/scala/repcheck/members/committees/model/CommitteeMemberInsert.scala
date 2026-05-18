package repcheck.members.committees.model

final case class CommitteeMemberInsert(
  committeeId: Long,
  memberId: Long,
  role: Option[String],
  side: Option[String],
  rank: Option[Int],
  congress: Int,
)

object CommitteeMemberInsert {

  private val ValidRoles: Set[String] =
    Set("Chairman", "Ranking Member", "Vice Chairman", "Member")

  def normalizeRole(raw: Option[String]): Option[String] =
    raw.map(_.trim).filter(ValidRoles.contains)

}
