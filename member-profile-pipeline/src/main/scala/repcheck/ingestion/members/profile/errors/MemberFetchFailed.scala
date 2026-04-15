package repcheck.ingestion.members.profile.errors

final case class MemberFetchFailed(
  bioguideId: Option[String],
  detail: String,
  cause: Option[Throwable] = None,
) extends Exception(s"Failed to fetch member ${bioguideId.getOrElse("<unknown>")}: $detail") {
  cause.foreach(initCause)
}
