package repcheck.ingestion.votes.errors

/**
 * Raised by [[repcheck.ingestion.votes.repo.VoteHistoryArchiver]] when the caller asks to archive a `votes` row that
 * does not exist in the live table. The caller (typically `VoteProcessor` from P3.1) is expected to archive only in the
 * "Updated" branch — where an existing row was found and is about to be overwritten. If this exception fires, the
 * caller's flow has a bug: it should not have invoked the archiver for a new vote or one whose row was concurrently
 * deleted.
 *
 * Kept distinct from [[VoteArchiveFailed]] so test assertions and operator dashboards can tell a missing-vote
 * precondition violation apart from a technical archive failure (constraint violation, DB outage, etc.).
 */
final case class VoteArchiveNotFound(voteId: Long) extends Exception(s"Cannot archive vote $voteId: no live row exists")
