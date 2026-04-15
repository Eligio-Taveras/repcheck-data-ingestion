package repcheck.members.lismapping.errors

final case class LisMappingUpsertFailed(
  lisId: String,
  detail: String,
  cause: Option[Throwable] = None,
) extends Exception(s"Failed to upsert LIS mapping for $lisId: $detail") {
  cause.foreach(initCause)
}
