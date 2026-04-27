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
 * Streams a Congress.gov bill text body straight from the HTTP response into the downstream extractor — no temp file
 * for HTML / XML / plain-text formats. Only PDF needs a temp file (PDF format requires random access to the xref table
 * at the end of the file), and that materialization is handled internally by
 * [[repcheck.ingestion.bills.text.extraction.PdfStreamExtractor]] so the downloader's API stays uniform: open HTTP,
 * emit response body bytes.
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
 * @param logger
 *   structured logger; download lifecycle events are emitted with the supplied correlation ID.
 */
class BillTextDownloader[F[_]: Async](
  client: Client[F],
  logger: PipelineLogger[F],
) {

  private val StepName = "bill-text-download"

  /**
   * Stream the response body bytes from `textUrl`. Status-code handling: 404 → [[TextDownloadFailed]], non-success
   * other than 404 → [[TextDownloadFailed]] including the response body for debugging, success → emit `response.body`.
   *
   * @param textUrl
   *   absolute URL of the bill text body to download. Parsed as an http4s `Uri`; an unparseable string raises
   *   [[InvalidTextUrl]] when the stream is first pulled.
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

    Stream.eval(parseUrl(textUrl)).flatMap { uri =>
      Stream.exec(logger.info(logCtx, s"Opening HTTP request to $textUrl (format=$textFormat)")) ++
        Stream.resource(client.run(Request[F](uri = uri))).flatMap { response =>
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

  private[download] def parseUrl(textUrl: String): F[Uri] =
    Async[F].fromEither(
      Uri.fromString(textUrl).leftMap(err => InvalidTextUrl(textUrl, err.sanitized))
    )

}
