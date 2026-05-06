package repcheck.ingestion.amendments.errors

/**
 * Raised by `AmendmentsApiClient` when a Congress.gov `/amendment` list or detail request fails after exhausting the
 * configured retry budget, OR when the response body cannot be parsed into the expected DTO. The natural key (e.g.
 * `"117-SAMDT-2137"`, or the request URL when the failure happened before identification) ties the failure to a
 * specific amendment for downstream skip / backoff logic; the cause carries the underlying network/HTTP/parse error.
 */
final case class AmendmentFetchFailed(
  naturalKey: String,
  cause: Throwable,
) extends Exception(
      s"Failed to fetch amendment $naturalKey: ${cause.getMessage}",
      cause,
    )
