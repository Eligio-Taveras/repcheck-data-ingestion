package repcheck.members.committees.pipeline

final case class CommitteeMembershipRefreshResult(
  committeesUpserted: Int,
  distinctMembersProcessed: Int,
  membershipRowsUpserted: Int,
)

object CommitteeMembershipRefreshResult {

  val empty: CommitteeMembershipRefreshResult =
    CommitteeMembershipRefreshResult(
      committeesUpserted = 0,
      distinctMembersProcessed = 0,
      membershipRowsUpserted = 0,
    )

}
