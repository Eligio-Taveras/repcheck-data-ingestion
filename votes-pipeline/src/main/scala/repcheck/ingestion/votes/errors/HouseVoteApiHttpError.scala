package repcheck.ingestion.votes.errors

import repcheck.ingestion.common.errors.HttpStatusError

/**
 * Raised at the HTTP boundary by the `HouseVotesApiClient` when the Congress.gov `/house-vote` endpoint returns a
 * non-2xx response. Carries the HTTP status code via the [[HttpStatusError]] trait so the generic
 * [[repcheck.ingestion.common.errors.HttpStatusErrorClassifier]] (specialised into [[HouseVoteApiErrorClassifier]]) can
 * classify transient (retry) vs systemic (fail fast) based on the status set.
 *
 * Retry exhaustion is wrapped by the retry wrapper's `errorFactory` into [[HouseVoteFetchFailed]] — that's the
 * user-facing terminal exception surfaced to callers of the client and contains the vote-level context (congress,
 * session, voteNumber).
 */
final case class HouseVoteApiHttpError(statusCode: Int, body: String)
    extends Exception(s"Congress.gov house-vote API returned HTTP $statusCode: $body")
    with HttpStatusError
