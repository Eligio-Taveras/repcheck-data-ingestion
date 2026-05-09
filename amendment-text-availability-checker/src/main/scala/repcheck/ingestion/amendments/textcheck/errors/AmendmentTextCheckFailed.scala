package repcheck.ingestion.amendments.textcheck.errors

/**
 * User-facing terminal exception surfaced to callers of `AmendmentTextApiClient` and the checker pipeline when the
 * `/amendment/.../text` request fails after exhausting the retry budget OR the response body cannot be parsed into the
 * expected DTO. The natural key (e.g. `"117-SAMDT-2137"`) ties the failure to a specific amendment for downstream skip
 * / backoff logic; the cause carries the underlying network/HTTP/parse error.
 */
final case class AmendmentTextCheckFailed(
  naturalKey: String,
  detail: String,
  cause: Throwable,
) extends Exception(
      s"Failed to check amendment text for $naturalKey: $detail",
      cause,
    )
