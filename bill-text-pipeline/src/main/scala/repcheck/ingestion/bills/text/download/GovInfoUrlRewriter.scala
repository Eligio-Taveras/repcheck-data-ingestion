package repcheck.ingestion.bills.text.download

import scala.util.matching.Regex

/**
 * Maps a Congress.gov-issued bill text URL to its equivalent on api.govinfo.gov.
 *
 * The Congress.gov `/bill/{c}/{type}/{n}/text` endpoint returns metadata about where each version of a bill's text
 * lives, but the URLs it hands us point at `www.congress.gov` — a Cloudflare-fronted public site that bot-challenges
 * raw HTTP clients with HTTP 403 + a "Just a moment..." JavaScript redirect page. That challenge fires unpredictably
 * (~30% of requests in the local stack), regardless of pacing.
 *
 * The same content is mirrored on the GPO's `api.govinfo.gov` under stable, authenticated package paths and is served
 * directly without an anti-bot layer — provided we send a registered API key. The mapping is mechanical: the package
 * identifier is the filename minus its extension, and the format suffix matches the URL extension.
 *
 * Examples:
 *
 * {{{
 *   https://www.congress.gov/119/bills/hr2384/BILLS-119hr2384rfs.htm
 *     → BILLS-119hr2384rfs / htm
 *
 *   https://www.congress.gov/118/bills/sjres89/BILLS-118sjres89pcs.xml
 *     → BILLS-118sjres89pcs / xml
 * }}}
 *
 * URLs that don't match the expected `BILLS-*.{htm|xml|pdf}` shape (very rare in practice — Congress.gov's formats
 * array is dominated by these three for legislation) return `None`. The downloader uses the original URL in that case,
 * which keeps backward compatibility for unforeseen Congress.gov-issued URLs at the cost of still tripping Cloudflare
 * for them. That's an acceptable fallback for the long tail.
 */
private[download] object GovInfoUrlRewriter {

  private val CongressGovBillUrlPattern: Regex =
    """^https?://(?:www\.)?congress\.gov/\d+/bills/[^/]+/(BILLS-[A-Za-z0-9]+)\.(htm|xml|pdf)(?:\?.*)?$""".r

  /**
   * Parse a Congress.gov bill text URL into its `(packageId, formatSuffix)` components, or return `None` if the input
   * doesn't match the expected shape.
   */
  def parseCongressGovBillUrl(originalUrl: String): Option[(String, String)] =
    CongressGovBillUrlPattern.findFirstMatchIn(originalUrl).map(m => (m.group(1), m.group(2)))

  /**
   * Build the api.govinfo.gov URL for a given package + format. The API key is appended by the downloader at request
   * time via `Uri.withQueryParam`, not embedded here, so this function stays pure and easy to test.
   */
  def toGovInfoUrl(packageId: String, formatSuffix: String, baseUrl: String): String =
    s"${baseUrl.stripSuffix("/")}/packages/$packageId/$formatSuffix"

}
