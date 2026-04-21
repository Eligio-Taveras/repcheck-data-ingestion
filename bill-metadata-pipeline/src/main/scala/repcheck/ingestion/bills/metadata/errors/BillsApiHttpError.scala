package repcheck.ingestion.bills.metadata.errors

import repcheck.ingestion.common.errors.HttpStatusError

/**
 * Raised by [[repcheck.ingestion.bills.metadata.api.BillsApiClient]] at the HTTP boundary when Congress.gov returns a
 * non-2xx response. Carries the HTTP status code (via the [[HttpStatusError]] trait) so [[BillsApiErrorClassifier]] can
 * decide whether the failure is transient (retry) or systemic (fail fast).
 *
 * Retry exhaustion wraps this (via the retry wrapper's `errorFactory`) in [[BillFetchFailed]], which is the user-facing
 * terminal exception surfaced to callers of the client.
 */
final case class BillsApiHttpError(statusCode: Int, body: String)
    extends Exception(s"Congress.gov bills API returned HTTP $statusCode: $body")
    with HttpStatusError
