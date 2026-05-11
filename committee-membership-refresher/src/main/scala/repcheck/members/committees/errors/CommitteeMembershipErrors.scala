package repcheck.members.committees.errors

final case class CommitteeFetchFailed(
  detail: String,
  cause: Option[Throwable] = None,
) extends Exception(s"Failed to fetch committee data: $detail") {
  cause.foreach(initCause)
}

final case class CommitteeUpsertFailed(
  committeeCode: String,
  detail: String,
  cause: Option[Throwable] = None,
) extends Exception(s"Failed to upsert committee $committeeCode: $detail") {
  cause.foreach(initCause)
}

final case class CommitteeMemberUpsertFailed(
  committeeCode: String,
  memberId: Long,
  detail: String,
  cause: Option[Throwable] = None,
) extends Exception(s"Failed to upsert committee member $committeeCode/member=$memberId: $detail") {
  cause.foreach(initCause)
}

final case class CommitteeApiHttpError(
  statusCode: Int,
  body: String,
) extends Exception(s"Congress.gov committee API returned HTTP $statusCode: $body")

final case class SenateCommitteeSoft404(
  committeeCode: String
) extends Exception(s"Senate committee XML returned HTML (soft-404) for $committeeCode")

final case class MemberResolutionFailed(
  bioguideId: String,
  detail: String,
) extends Exception(s"Failed to resolve member $bioguideId: $detail")
