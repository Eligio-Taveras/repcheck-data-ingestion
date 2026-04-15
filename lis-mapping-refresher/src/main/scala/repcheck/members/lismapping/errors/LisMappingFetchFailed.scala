package repcheck.members.lismapping.errors

final case class LisMappingFetchFailed(
  detail: String,
  cause: Option[Throwable] = None,
) extends Exception(s"Failed to fetch LIS mapping: $detail") {
  cause.foreach(initCause)
}
