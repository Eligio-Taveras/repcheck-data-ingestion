package repcheck.ingestion.bills.text.download

import java.util.UUID

import cats.effect.Async
import cats.syntax.all._

import org.http4s.client.Client
import org.http4s.{Request, Status, Uri}

import fs2.Stream

import repcheck.ingestion.bills.text.errors.{InvalidTextUrl, TextDownloadFailed}
import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}

/**
 * Streams a bill text body from GPO's `api.govinfo.gov` straight into the downstream extractor — no temp file for HTML
 * / XML / plain-text formats. Only PDF needs a temp file (PDF format requires random access to the xref table at the
 * end of the file), and that materialization is handled internally by
 * [[repcheck.ingestion.bills.text.extraction.PdfStreamExtractor]] so the downloader's API stays uniform: open HTTP,
 * emit response body bytes.
 *
 * ==Source: GovInfo, not Congress.gov==
 *
 * Congress.gov's `/bill/.../text` endpoint returns metadata pointing at `www.congress.gov/{c}/bills/.../BILLS-*.htm`,
 * which is fronted by Cloudflare. Cloudflare bot-challenges raw HTTP clients (no JS, no cookies) with HTTP 403 + a
 * "Just a moment..." page roughly 30% of the time, regardless of pacing. The same content is mirrored on GPO's
 * `api.govinfo.gov` under stable, key-authenticated package paths — no anti-bot layer. We rewrite each incoming
 * Congress.gov URL to its GovInfo equivalent before issuing the request. URLs that don't match the
 * `BILLS-*.{htm|xml|pdf}` shape fall through to the original URL (acceptable long-tail fallback).
 *
 * ==Why streaming, no buffering==
 *
 * Pre-Phase-3 the downloader wrote every body to a temp file regardless of format, then handed the path to a
 * format-specific extractor. After the streaming-extractor refactor, HTML/XML/plaintext extractors can consume a
 * `Stream[F, Byte]` directly via their respective parsers' InputStream support, so the temp-file step adds disk I/O for
 * no benefit on those formats. Phase 3 cuts the disk hop and pipes HTTP body bytes through extraction → chunking →
 * embedding → INSERT inside one fs2 stream.
 *
 * ==Backpressure==
 *
 * The returned `Stream[F, Byte]` is pull-based — the consumer (extractor parser) decides how fast bytes arrive. If
 * embedding (5+ s per batch) is the slowest stage, fs2 backpressures all the way back to the HTTP socket; the server
 * slows or pauses sending. TCP-level buffering absorbs the modest mismatch; nothing accumulates in our heap.
 *
 * ==Connection lifecycle==
 *
 * The HTTP request opens lazily when the stream is first pulled and closes when the stream completes (success or
 * error). Mid-stream cancellation aborts the response. fs2's `Stream.resource` handles all of this; no manual cleanup
 * needed.
 *
 * ==Timeout==
 *
 * No application-level timeout on the body read. With extraction interleaved with embedding (minutes per bill), a
 * 60-second body-read timeout (the pre-Phase-3 default) would kill long-running pipelines mid-stream. Stuck connections
 * are caught by http4s client's idle/read timeouts (configured at client builder time); a truly runaway-but-trickling
 * server is bounded by the HTTP-level retry/redelivery loop at the Pub/Sub layer.
 *
 * @param client
 *   the http4s `Client[F]` used for the request. Caller is responsible for any rate-limit wrapping; this downloader
 *   doesn't paginate or retry.
 * @param govInfoApiKey
 *   GovInfo API key issued by GPO's `govinfo.gov/api-signup`. Appended as `?api_key=...` on every rewritten GovInfo
 *   request. Not used for non-rewrite (passthrough) URLs.
 * @param govInfoBaseUrl
 *   base URL for the GovInfo API; defaults to the production endpoint, overridable for tests pointing at WireMock.
 * @param logger
 *   structured logger; download lifecycle events are emitted with the supplied correlation ID.
 */
class BillTextDownloader[F[_]: Async](
  client: Client[F],
  govInfoApiKey: String,
  govInfoBaseUrl: String,
  logger: PipelineLogger[F],
) {

  private val StepName = "bill-text-download"

  /**
   * Stream the response body bytes from `textUrl`. The URL is rewritten to its GovInfo equivalent when it matches the
   * Congress.gov `BILLS-*.{htm|xml|pdf}` shape. Status-code handling: 404 → [[TextDownloadFailed]], non-success other
   * than 404 → [[TextDownloadFailed]] including the response body for debugging, success → emit `response.body`.
   *
   * @param textUrl
   *   URL emitted by the Congress.gov `/bill/.../text` API for a chosen version + format. If it points at
   *   `www.congress.gov/.../BILLS-*.{htm|xml|pdf}`, it's rewritten to the GovInfo package endpoint with the API key.
   *   Otherwise it's used as-is.
   * @param textFormat
   *   format hint from Congress.gov (e.g. `"Formatted Text"`, `"PDF"`). Used for log context only — the actual format
   *   dispatch happens in [[repcheck.ingestion.bills.text.extraction.BillTextExtractor]].
   * @param correlationId
   *   correlation ID for log threading; flows through every emitted log line in this download.
   */
  def streamBody(
    textUrl: String,
    textFormat: String,
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
        Stream.exec(logger.info(logCtx, s"Opening HTTP request to $effectiveUrl (format=$textFormat)")) ++
          Stream.resource(client.run(request)).flatMap { response =>
            response.status match {
              case Status.NotFound =>
                Stream.raiseError[F](TextDownloadFailed(textUrl, textFormat, "HTTP 404 - bill text not found"))
              case status if status.isSuccess =>
                response.body
              case status =>
                Stream.eval(response.as[String]).flatMap { body =>
                  Stream.raiseError[F](TextDownloadFailed(textUrl, textFormat, s"HTTP ${status.code}: $body"))
                }
            }
          }
    }
  }

  /**
   * Translates the caller-supplied URL into the actual `Request[F]` we issue, applying the GovInfo URL rewrite +
   * api_key parameter when the input matches the BILLS-* pattern. Returns the request alongside the effective URL
   * string used for logging — note we deliberately log a redacted form (without the api_key) so secrets don't leak into
   * log aggregators.
   */
  private[download] def buildRequest(textUrl: String): F[(Request[F], String)] =
    GovInfoUrlRewriter.parseCongressGovBillUrl(textUrl) match {
      case Some((packageId, suffix)) =>
        val govInfoUrl = GovInfoUrlRewriter.toGovInfoUrl(packageId, suffix, govInfoBaseUrl)
        parseUrl(govInfoUrl).map { uri =>
          val authedUri = uri.withQueryParam("api_key", govInfoApiKey)
          (Request[F](uri = authedUri), govInfoUrl)
        }
      case None =>
        parseUrl(textUrl).map(uri => (Request[F](uri = uri), textUrl))
    }

  private[download] def parseUrl(textUrl: String): F[Uri] =
    Async[F].fromEither(
      Uri.fromString(textUrl).leftMap(err => InvalidTextUrl(textUrl, err.sanitized))
    )

}
