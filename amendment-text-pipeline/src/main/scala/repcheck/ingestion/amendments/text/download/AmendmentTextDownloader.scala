package repcheck.ingestion.amendments.text.download

import java.util.UUID

import cats.effect.Async
import cats.syntax.all._

import org.http4s.client.Client
import org.http4s.{Request, Status, Uri}

import fs2.Stream

import repcheck.ingestion.amendments.text.errors.{AmendmentTextDownloadFailed, InvalidAmendmentTextUrl}
import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}

/**
 * Streams an amendment text body from GPO's `api.govinfo.gov` (CREC collection) straight into the downstream extractor.
 * Mirrors [[repcheck.ingestion.bills.text.download.BillTextDownloader]] but for the CREC URL pattern.
 *
 * URLs received in `amendment.text.available` events point at `www.congress.gov/.../crec/...` (Cloudflare-fronted,
 * unsuitable for raw HTTP clients). [[CrecGovInfoUrlRewriter]] maps them to api.govinfo.gov package URLs which serve
 * the same content directly under `?api_key=...`. URLs that don't match the rewriter regex are used as-is — the
 * operator monitors the `rewriter_miss` outcome counter to detect when upstream URL shape shifts.
 *
 * Streaming + backpressure semantics are identical to the bill-side downloader: the body is pulled lazily, fs2
 * backpressures back through the HTTP socket if downstream stages (extract → chunk → embed → INSERT) are slower than
 * the network. No application-level body-read timeout — long bills/amendments may take minutes to fully embed and the
 * http4s client's idle-connection timeouts handle truly stuck connections.
 */
class AmendmentTextDownloader[F[_]: Async](
  client: Client[F],
  govInfoApiKey: String,
  govInfoBaseUrl: String,
  logger: PipelineLogger[F],
) {

  private val StepName = "amendment-text-download"

  /**
   * Stream the response body bytes for `textUrl`. Status-code handling mirrors the bill-side: 404 →
   * [[AmendmentTextDownloadFailed]], non-success other than 404 → [[AmendmentTextDownloadFailed]] including the
   * response body for debugging, success → emit `response.body`.
   *
   * @param textUrl
   *   URL emitted in the event for the chosen `(versionType, formatType)` tuple. Rewritten to api.govinfo.gov when it
   *   matches the CREC pattern, otherwise used as-is.
   * @param formatType
   *   `"HTML"` or `"PDF"` (the only two values §7.5 emits). Used for log context only — actual extraction dispatch
   *   happens in [[repcheck.ingestion.amendments.text.extraction.AmendmentTextExtractor]].
   * @param correlationId
   *   correlation ID flowed through every emitted log line.
   */
  def streamBody(
    textUrl: String,
    formatType: String,
    correlationId: UUID,
  ): Stream[F, Byte] = {
    val logCtx = LogContext(
      runId = correlationId.toString,
      stepName = StepName,
      correlationId = Some(correlationId),
      entityId = Some(textUrl),
    )

    Stream.eval(buildRequest(textUrl)).flatMap {
      case (request, effectiveUrl) =>
        Stream.exec(logger.info(logCtx, s"Opening HTTP request to $effectiveUrl (format=$formatType)")) ++
          Stream.resource(client.run(request)).flatMap { response =>
            response.status match {
              case Status.NotFound =>
                Stream.raiseError[F](
                  AmendmentTextDownloadFailed(textUrl, formatType, "HTTP 404 - amendment text not found")
                )
              case status if status.isSuccess =>
                response.body
              case status =>
                Stream.eval(response.as[String]).flatMap { body =>
                  Stream.raiseError[F](AmendmentTextDownloadFailed(textUrl, formatType, s"HTTP ${status.code}: $body"))
                }
            }
          }
    }
  }

  /**
   * Translate the caller-supplied URL into the actual `Request[F]`. Returns the request alongside a redacted effective
   * URL string (without the api_key) for logging — matches the bill-side downloader to keep logs free of secret
   * material.
   */
  private[download] def buildRequest(textUrl: String): F[(Request[F], String)] =
    CrecGovInfoUrlRewriter.parseCongressGovCrecUrl(textUrl) match {
      case Some((packageId, granuleId, formatSuffix)) =>
        val govInfoUrl = CrecGovInfoUrlRewriter.toGovInfoUrl(packageId, granuleId, formatSuffix, govInfoBaseUrl)
        parseUrl(govInfoUrl).map { uri =>
          val authedUri = uri.withQueryParam("api_key", govInfoApiKey)
          (Request[F](uri = authedUri), govInfoUrl)
        }
      case None =>
        parseUrl(textUrl).map(uri => (Request[F](uri = uri), textUrl))
    }

  private[download] def parseUrl(textUrl: String): F[Uri] =
    Async[F].fromEither(
      Uri.fromString(textUrl).leftMap(err => InvalidAmendmentTextUrl(textUrl, err.sanitized))
    )

  /**
   * Pure preview of the URL the downloader will GET — used by the processor to populate
   * `amendment_text_versions.download_url` for audit. Returns the rewritten api.govinfo.gov URL when the input matches
   * the CREC pattern; `None` otherwise (the downloader uses the source URL as-is in that case, so the stored
   * `download_url` is left NULL — matches the spec's L7 acceptance criterion). No api_key is embedded — the storage
   * column should not log secrets.
   */
  def previewDownloadUrl(textUrl: String): Option[String] =
    CrecGovInfoUrlRewriter.parseCongressGovCrecUrl(textUrl).map {
      case (packageId, granuleId, formatSuffix) =>
        CrecGovInfoUrlRewriter.toGovInfoUrl(packageId, granuleId, formatSuffix, govInfoBaseUrl)
    }

}
