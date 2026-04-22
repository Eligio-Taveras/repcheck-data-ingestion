package repcheck.ingestion.votes.errors

/**
 * Terminal, user-facing exception raised by [[repcheck.ingestion.votes.api.HouseVotesApiClient]] once the retry wrapper
 * gives up, or when we detect a non-retriable failure (decoder failure, bad URL, 404 on a specific vote). Carries the
 * vote-level context (`congress`, `session`, optional `voteNumber` for detail endpoints) so callers and log aggregators
 * can identify exactly which vote failed without having to reconstruct the URL.
 *
 * Unique simple name across the repo per the `sbt-exception-uniqueness` plugin v0.5.0. The `cause` is normally an
 * [[HouseVoteApiHttpError]] (status-coded) or a decoder/IO error propagated from http4s / circe; downstream classifiers
 * only see this wrapper once retry has exhausted, so we do not implement `HttpStatusError` here — the inner
 * [[HouseVoteApiHttpError]] carries that information for the retry wrapper's classifier.
 */
final case class HouseVoteFetchFailed(
  congress: Int,
  session: Int,
  voteNumber: Option[Int],
  detail: String,
  cause: Throwable,
) extends Exception(
      HouseVoteFetchFailed.renderMessage(congress, session, voteNumber, detail),
      cause,
    )

object HouseVoteFetchFailed {

  /**
   * Build the human-readable message in one place so callers of `getMessage` and the case class's `toString` agree, and
   * tests can assert the identifiers are all present. Message format is stable and intentionally includes all three
   * identifiers for log-grep friendliness; any change here should update the corresponding unit tests.
   */
  def renderMessage(congress: Int, session: Int, voteNumber: Option[Int], detail: String): String = {
    val voteSuffix = voteNumber.fold("")(v => s", vote=$v")
    s"Failed to fetch House vote (congress=$congress, session=$session$voteSuffix): $detail"
  }

}
