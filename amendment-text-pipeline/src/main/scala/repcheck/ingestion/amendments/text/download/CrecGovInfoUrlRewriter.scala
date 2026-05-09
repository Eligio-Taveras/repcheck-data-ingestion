package repcheck.ingestion.amendments.text.download

import scala.util.matching.Regex

/**
 * Maps a Congress.gov-issued amendment text URL (CREC mirror) to its equivalent on api.govinfo.gov.
 *
 * Mirror of [[repcheck.ingestion.bills.text.download.GovInfoUrlRewriter]] but for the Congressional Record (CREC)
 * collection rather than the BILLS collection. Pre-flight verification (per §7.6 P7.1, 2026-05-03):
 *
 * `HEAD https://api.govinfo.gov/packages/CREC-2021-08-01/granules/CREC-2021-08-01-pt1-PgS5255/htm?api_key=...` → `200
 * OK, Content-Type: text/html, Content-Length: 3MB`. No Cloudflare interaction. Govinfo rate limit is documented at
 * 36000/hour per `X-Ratelimit-Limit`.
 *
 * Pattern variants observed in production:
 *
 * {{{
 *   https://www.congress.gov/117/crec/2021/08/01/167/136/CREC-2021-08-01-pt1-PgS5255.htm
 *   https://www.congress.gov/117/crec/2021/08/01/167/136/modified/CREC-2021-08-01-pt1-PgS5255.htm
 *   https://www.congress.gov/119/crec/2025/02/19/171/33/modified/CREC-2025-02-19-pt1-PgS1044-4.htm
 * }}}
 *
 * URLs that don't match (`None` from [[parseCongressGovCrecUrl]]) fall through to the original URL — the downloader
 * uses `www.congress.gov` directly for that long-tail and the operator monitors the `rewriter_miss` outcome counter.
 */
private[download] object CrecGovInfoUrlRewriter {

  // Captures: (packageId="CREC-2021-08-01", granuleSuffix="-pt1-PgS5255", formatSuffix="htm"|"pdf").
  // Optional `modified/` segment per the variant observed in production. Page suffix accommodates both
  // `PgS5255` (no trailing dash-N) and `PgS1044-4` (with trailing dash-N).
  private[download] val CongressGovCrecUrlPattern: Regex =
    """^https?://(?:www\.)?congress\.gov/\d+/crec/\d+/\d+/\d+/\d+/\d+/(?:modified/)?(CREC-\d{4}-\d{2}-\d{2})(-pt\d+-Pg[A-Za-z0-9-]+)\.(htm|pdf)(?:\?.*)?$""".r

  /**
   * Parse a Congress.gov CREC URL into `(packageId, granuleId, formatSuffix)` for rewriting to api.govinfo.gov.
   *
   * Returns `None` for URLs that don't match — including BILLS URLs (we have a separate rewriter for those), malformed
   * URLs, and URLs containing path-traversal segments like `..`. The latter is a defensive guard so an adversarial
   * event payload can't redirect the downloader to an unrelated path under api.govinfo.gov; the regex already
   * constrains the structure but explicit rejection of `..` keeps the contract clean.
   */
  def parseCongressGovCrecUrl(originalUrl: String): Option[(String, String, String)] =
    if (originalUrl.contains("..")) {
      None
    } else {
      CongressGovCrecUrlPattern.findFirstMatchIn(originalUrl).map { m =>
        val pkg           = m.group(1)          // "CREC-2021-08-01"
        val granuleSuffix = m.group(2)          // "-pt1-PgS5255"
        val granule       = pkg + granuleSuffix // "CREC-2021-08-01-pt1-PgS5255"
        val ext           = m.group(3)          // "htm" | "pdf"
        (pkg, granule, ext)
      }
    }

  /**
   * Build the api.govinfo.gov URL for a CREC granule. Pure — no IO, no API key embedded. The downloader appends
   * `?api_key=...` at request time via `Uri.withQueryParam` so the API key never lands in logs or stack traces.
   */
  def toGovInfoUrl(packageId: String, granuleId: String, formatSuffix: String, baseUrl: String): String =
    s"${baseUrl.stripSuffix("/")}/packages/$packageId/granules/$granuleId/$formatSuffix"

}
