package repcheck.ingestion.amendments.textcheck.errors

import repcheck.ingestion.common.errors.HttpStatusError

/**
 * Raised by `AmendmentTextApiClient` at the HTTP boundary when Congress.gov's `/amendment/.../text` endpoint returns a
 * non-2xx response. Carries the HTTP status code (via [[HttpStatusError]]), the (possibly empty) response body, and the
 * 1-indexed attempt number — same pattern as the §7.1 `AmendmentsApiHttpError`, but a distinct type so observability
 * counters and stack traces stay unambiguous about which Congress.gov call site failed.
 */
final case class AmendmentTextCheckHttpError(statusCode: Int, body: String, attempt: Int)
    extends Exception(s"Amendment text API HTTP error $statusCode (attempt $attempt): $body")
    with HttpStatusError
