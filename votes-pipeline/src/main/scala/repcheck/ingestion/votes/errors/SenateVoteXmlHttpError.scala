package repcheck.ingestion.votes.errors

import repcheck.ingestion.common.errors.HttpStatusError

/**
 * Raised at the HTTP boundary of [[repcheck.ingestion.votes.xml.SenateVoteXmlClient]] when senate.gov returns a non-2xx
 * response. Carries the HTTP status code (via [[HttpStatusError]]) so [[SenateVoteXmlErrorClassifier]] can decide
 * whether the failure is transient (retry) or systemic (fail fast). Retry exhaustion wraps this in
 * [[SenateVoteFetchFailed]] via the retry wrapper's `errorFactory`.
 */
final case class SenateVoteXmlHttpError(statusCode: Int, body: String)
    extends Exception(s"senate.gov roll-call XML request returned HTTP $statusCode: $body")
    with HttpStatusError
