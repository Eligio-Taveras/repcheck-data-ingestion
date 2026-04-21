package repcheck.ingestion.votes.errors

/**
 * Raised by [[repcheck.ingestion.votes.repo.VoteRepository]] when an `INSERT ... ON CONFLICT ... DO UPDATE` against the
 * `votes` table fails. The unique message anchor (`"Failed to upsert vote ..."`) lets `VoteErrorClassifier` distinguish
 * persistence failures from other vote-pipeline errors when deciding between `Transient` (retry the run item) and
 * `Systemic` (halt the run).
 *
 * The `naturalKey` captures the `{chamber}:{congress}:{session}:{rollNumber}` natural key of the vote being upserted so
 * operators can correlate the failure with a specific roll call. `detail` holds a human-readable description of the
 * underlying JDBC failure, and `cause` wraps the original `SQLException` when Doobie surfaces one.
 */
final case class VoteUpsertFailed(
  naturalKey: String,
  detail: String,
  cause: Option[Throwable] = None,
) extends Exception(s"Failed to upsert vote $naturalKey: $detail") {
  cause.foreach(initCause)
}
