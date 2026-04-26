package repcheck.ingestion.bills.summary.errors

/**
 * Raised by `BillSummariesApiClient` when a Congress.gov `/summaries` request fails after exhausting the configured
 * retry budget, OR when the response body cannot be parsed into the expected DTO. Surfaces the endpoint URL (with the
 * `api_key` query parameter stripped via `Uri.removeQueryParam("api_key")` to keep secrets out of logs) and the HTTP
 * status code (`0` when the failure happened before a response arrived — connection error, JSON parse failure, etc.).
 */
final case class BillSummaryFetchFailed(
  endpoint: String,
  statusCode: Int,
  detail: String,
  cause: Throwable,
) extends Exception(
      s"Failed to fetch bill summary from $endpoint (status=$statusCode): $detail",
      cause,
    )
