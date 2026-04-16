package repcheck.ingestion.members.profile.errors

/**
 * Raised by the member profile pipeline when a per-member processing step fails in a way that is specific to the
 * processor (e.g., DTO-to-DO conversion, post-transaction re-reads that contradict the expected state). Fetch failures
 * raise [[MemberFetchFailed]]; repository/persistence failures surface via the underlying doobie/DB exceptions.
 */
final case class MemberProcessingFailed(
  bioguideId: String,
  detail: String,
  cause: Option[Throwable] = None,
) extends Exception(s"Failed to process member $bioguideId: $detail") {
  cause.foreach(initCause)
}
