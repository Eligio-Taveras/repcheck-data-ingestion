package repcheck.ingestion.bills.summary.errors

import repcheck.ingestion.common.errors.HttpStatusError

/**
 * Raised by `BillSummariesApiClient` on a non-success HTTP response from Congress.gov. Carries the status code and the
 * (raw, possibly empty) response body. Classified by `BillSummariesApiErrorClassifier` into Transient (retry) or
 * Systemic (halt) buckets via the shared `HttpStatusError` marker trait.
 */
final case class BillSummariesApiHttpError(statusCode: Int, body: String)
    extends Exception(s"Bill summaries API HTTP error $statusCode: $body")
    with HttpStatusError
