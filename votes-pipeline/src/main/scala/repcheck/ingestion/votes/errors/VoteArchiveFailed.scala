package repcheck.ingestion.votes.errors

/**
 * Raised by [[repcheck.ingestion.votes.repo.VoteHistoryArchiver]] when copying the live `votes` row and its
 * `vote_positions` children into the history tables fails. Surfaces the BIGSERIAL `voteId` of the live row that the
 * processor asked to archive so operators can look it up in logs and the database.
 *
 * The archiver normally succeeds silently because it runs inside the caller's transaction — this exception is reserved
 * for the rare case where the history INSERT itself fails (e.g., constraint violation on `vote_history` or
 * `vote_history_positions`). On failure the caller's outer transaction rolls back, so the live `votes` row is never
 * overwritten with data that could not be archived.
 */
final case class VoteArchiveFailed(
  voteId: Long,
  detail: String,
  cause: Option[Throwable] = None,
) extends Exception(s"Failed to archive vote $voteId: $detail") {
  cause.foreach(initCause)
}
