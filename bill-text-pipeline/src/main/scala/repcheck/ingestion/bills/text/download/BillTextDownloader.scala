package repcheck.ingestion.bills.text.download

import java.nio.file.Path
import java.util.UUID

import scala.concurrent.duration._

import cats.effect.{Async, Resource}
import cats.syntax.all._

import org.http4s.client.Client
import org.http4s.{Request, Status, Uri}

import fs2.io.file.{Files, Flags, Path => FsPath}

import repcheck.ingestion.bills.text.config.BillTextPipelineConfig
import repcheck.ingestion.bills.text.errors.{InvalidTextUrl, TextDownloadFailed, TextDownloadTimedOut}
import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}

/**
 * Streams a Congress.gov bill text body to disk as a temp file rather than buffering the whole response in heap as a
 * `String`. The `downloadToTempFile` `Resource[F, Path]` owns the temp-file lifecycle: created on `acquire`, deleted on
 * `release`, regardless of whether the body completes successfully or the F effect fails partway through.
 *
 * ==Why streaming-to-temp-file==
 *
 * The pre-revision flow (`response.as[String]`) materialized the entire body into heap before any extraction or
 * chunking could begin. That set a hard 10-MiB ceiling enforced by an in-memory byte counter, which 32 STATUTE PDFs in
 * production promptly exceeded. The new flow pipes `response.body: Stream[F, Byte]` straight to
 * `Files[F].writeAll(tempPath)` so bytes flow through the OS page cache in ~64 KB chunks; in-heap allocation per
 * request is bounded by fs2's chunk size, not the body size.
 *
 * ==No body-size cap==
 *
 * The downloader itself imposes no upper bound on body size. The pre-revision `max-content-bytes` cap was a vestige of
 * the buffered-`String` heap protection — once heap is bounded by chunk size regardless of body size, the cap protects
 * nothing the streaming flow doesn't already cover. Runaway-body protection is a layered concern handled elsewhere:
 *
 *   - `downloadTimeoutSeconds` aborts a stream that's running too long (slow loris, infinite body).
 *   - `Files.writeAll` raises `IOException("No space left on device")` if the temp partition fills, classified
 *     Transient by [[repcheck.ingestion.bills.text.pipeline.BillTextProcessor.classifyError]].
 *   - The Resource release deletes whatever was written before the failure, so a partial-write on disk-full doesn't
 *     leak.
 *
 * The current real upper bound on body size is the **extraction layer's** heap budget — Jsoup/scala-xml/PDFBox parse
 * the file fully into memory. That's the architectural ceiling and the place to address if we ever need to process
 * truly arbitrary-size bodies (Phase 3 streaming-extraction work).
 *
 * @param client
 *   the http4s `Client[F]` used for the request. Caller is responsible for any rate-limit wrapping; this downloader
 *   doesn't paginate or retry.
 * @param config
 *   pipeline config supplying `downloadTimeoutSeconds`.
 * @param logger
 *   structured logger; download lifecycle events are emitted with the supplied correlation ID.
 */
class BillTextDownloader[F[_]: Async](
  client: Client[F],
  config: BillTextPipelineConfig,
  logger: PipelineLogger[F],
) {

  private val StepName = "bill-text-download"

  /**
   * Resource that downloads `textUrl` into a temp file and yields the file's `Path`. The temp file is created in the
   * default tmp directory with prefix `bill-text-` (so operators can identify orphaned files if cleanup misbehaves) and
   * is automatically deleted when the Resource closes — including on failure.
   *
   * Acquire phase: open the temp file, run the HTTP request, pipe the response body through the size-validating counter
   * into `writeAll`. Returns the `Path`. Release phase: delete the temp file (idempotent — `deleteIfExists` tolerates a
   * missing file).
   *
   * @param textUrl
   *   absolute URL of the bill text body to download. Parsed as an http4s `Uri`; an unparseable string raises
   *   [[InvalidTextUrl]] on Resource acquisition.
   * @param textFormat
   *   format hint from Congress.gov (e.g. `"Formatted Text"`, `"PDF"`). Used for log context only — the actual format
   *   detection happens during extraction in [[repcheck.ingestion.bills.text.extraction.BillTextExtractor]].
   * @param correlationId
   *   correlation ID for log threading; flows through every emitted log line in this download.
   */
  def downloadToTempFile(
    textUrl: String,
    textFormat: String,
    correlationId: UUID,
  ): Resource[F, Path] = {
    val logCtx = LogContext(
      runId = correlationId.toString,
      stepName = StepName,
      correlationId = Some(correlationId),
      entityId = Some(textUrl),
    )

    Files
      .forAsync[F]
      .tempFile(dir = None, prefix = "bill-text-", suffix = ".bin", permissions = None)
      .evalMap { tempPath =>
        for {
          uri <- parseUrl(textUrl)
          _   <- logger.info(logCtx, s"Streaming bill text from $textUrl (format=$textFormat) to $tempPath")
          _   <- streamBodyToFile(uri, textUrl, textFormat, tempPath, logCtx)
          _   <- logger.info(logCtx, s"Bill text written to $tempPath")
        } yield tempPath.toNioPath
      }
  }

  private[download] def parseUrl(textUrl: String): F[Uri] =
    Async[F].fromEither(
      Uri.fromString(textUrl).leftMap(err => InvalidTextUrl(textUrl, err.sanitized))
    )

  /**
   * Run the HTTP request and pipe the response body straight to the supplied temp file. The timeout wraps the entire
   * body-reading effect; an over-long download fails fast with [[TextDownloadTimedOut]]. Status-code handling preserves
   * the prior contract: 404 → [[TextDownloadFailed]], other non-success → [[TextDownloadFailed]] with the body text
   * included for debugging.
   */
  private[download] def streamBodyToFile(
    uri: Uri,
    textUrl: String,
    textFormat: String,
    tempPath: FsPath,
    logCtx: LogContext,
  ): F[Unit] = {
    val timeout = config.downloadTimeoutSeconds.seconds
    val _       = logCtx // reserved for future per-status logging

    Async[F].timeoutTo(
      fa = client.run(Request[F](uri = uri)).use { response =>
        response.status match {
          case Status.NotFound =>
            Async[F].raiseError[Unit](TextDownloadFailed(textUrl, textFormat, "HTTP 404 - bill text not found"))
          case status if status.isSuccess =>
            response.body
              .through(Files.forAsync[F].writeAll(tempPath, Flags.Write))
              .compile
              .drain
          case status =>
            response.as[String].flatMap { body =>
              Async[F].raiseError[Unit](TextDownloadFailed(textUrl, textFormat, s"HTTP ${status.code}: $body"))
            }
        }
      },
      duration = timeout,
      fallback = Async[F].raiseError(TextDownloadTimedOut(textUrl, config.downloadTimeoutSeconds)),
    )
  }

}
