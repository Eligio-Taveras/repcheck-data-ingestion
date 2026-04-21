package repcheck.ingestion.members.profile.errors

/**
 * Raised by [[repcheck.ingestion.members.profile.api.MembersApiClient]] at the HTTP boundary when Congress.gov returns
 * a non-2xx response. Carries the HTTP status code so [[MembersApiErrorClassifier]] can decide whether the failure is
 * transient (retry) or systemic (fail fast).
 *
 * Retry exhaustion wraps this (via the retry wrapper's `errorFactory`) in [[MemberFetchFailed]], which is the
 * user-facing terminal exception surfaced to callers of the client.
 */
final case class MembersApiHttpError(statusCode: Int, body: String)
    extends Exception(s"Congress.gov members API returned HTTP $statusCode: $body")
