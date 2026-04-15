package repcheck.members.common.errors

final case class MemberUpsertFailed(
  bioguideId: String,
  detail: String,
  cause: Option[Throwable] = None,
) extends Exception(s"Failed to upsert member $bioguideId: $detail") {
  cause.foreach(initCause)
}
