package repcheck.ingestion.bills.textcheck.errors

/**
 * Raised by [[repcheck.ingestion.bills.textcheck.api.BillTextApiClient]] at the HTTP boundary when Congress.gov returns
 * a non-2xx response. Carries the HTTP status code so [[BillTextApiErrorClassifier]] can decide whether the failure is
 * transient (retry) or systemic (fail fast).
 *
 * Retry exhaustion wraps this (via the retry wrapper's `errorFactory`) in [[BillTextCheckFailed]], which is the
 * user-facing terminal exception surfaced to callers of the client.
 */
final case class BillTextApiHttpError(statusCode: Int, body: String)
    extends Exception(s"Congress.gov bill-text API returned HTTP $statusCode: $body")
