package repcheck.members.committees.pipeline

final case class CommitteeMembershipRefreshResult(
  committeesUpserted: Int,
  houseMembersProcessed: Int,
  senateMembersProcessed: Int,
  membershipRowsUpserted: Int,
)

object CommitteeMembershipRefreshResult {

  val empty: CommitteeMembershipRefreshResult =
    CommitteeMembershipRefreshResult(
      committeesUpserted = 0,
      houseMembersProcessed = 0,
      senateMembersProcessed = 0,
      membershipRowsUpserted = 0,
    )

}
