package repcheck.ingestion.amendments.text.errors

import repcheck.ingestion.common.errors.HttpStatusError

/**
 * Marker exception for non-success HTTP responses from `api.govinfo.gov`. Carries the status code so the shared
 * [[repcheck.ingestion.common.errors.HttpStatusErrorClassifier]] can route 429 / 5xx to Transient and the rest (401/403
 * — invalid api_key — and other 4xx) to Systemic.
 *
 * Distinct from [[AmendmentTextDownloadFailed]]: this typed error is used for non-404, non-success HTTP responses so
 * the retry wrapper can make the Transient/Systemic decision via [[HttpStatusError.statusCode]].
 * [[AmendmentTextDownloadFailed]] remains the generic download failure surfaced through the FS2 stream channel for
 * other failures such as 404s and non-HTTP issues like parse errors.
 */
final case class AmendmentTextDownloadHttpError(statusCode: Int, body: String)
    extends Exception(s"Amendment text download HTTP error $statusCode: $body")
    with HttpStatusError
