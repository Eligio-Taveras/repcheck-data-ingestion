package repcheck.ingestion.amendments.errors

import repcheck.ingestion.common.errors.HttpStatusError

/**
 * Raised by `AmendmentsApiClient` on a non-success HTTP response from Congress.gov. Carries the status code, the (raw,
 * possibly empty) response body, and the 1-indexed attempt number that produced this failure (for retry-budget tracking
 * in logs). Classified by `AmendmentsApiErrorClassifier` into Transient (retry) or Systemic (halt) buckets via the
 * shared [[HttpStatusError]] marker trait.
 */
final case class AmendmentsApiHttpError(statusCode: Int, body: String, attempt: Int)
    extends Exception(s"Amendments API HTTP error $statusCode (attempt $attempt): $body")
    with HttpStatusError
