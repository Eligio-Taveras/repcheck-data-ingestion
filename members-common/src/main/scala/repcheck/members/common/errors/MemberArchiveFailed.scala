package repcheck.members.common.errors

final case class MemberArchiveFailed(
  bioguideId: String,
  detail: String,
  cause: Option[Throwable] = None,
) extends Exception(s"Failed to archive member $bioguideId: $detail") {
  cause.foreach(initCause)
}
