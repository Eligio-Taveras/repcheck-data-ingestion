package repcheck.ingestion.votes.errors

/**
 * Cause attached to [[SenateVoteFetchFailed]] when the caller passes a vote number that cannot be represented in
 * senate.gov's 5-digit URL segment (i.e., not in `[1, 99999]`). Surfaced as a cause (rather than raised directly) so
 * the outer [[SenateVoteFetchFailed]] message carries the same congress/session context as every other fetch failure
 * and the root-cause chain stays intact in logs.
 */
final case class SenateVoteNumberOutOfRange(voteNumber: Int, reason: String)
    extends Exception(s"Senate vote number ${voteNumber.toString} is out of range: $reason")
