package com.repcheck.bills.text.download

import java.util.UUID

import scala.concurrent.duration._

import cats.effect.Async
import cats.syntax.all._

import org.http4s.client.Client
import org.http4s.{Request, Status, Uri}

import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}

import com.repcheck.bills.text.config.BillTextPipelineConfig
import com.repcheck.bills.text.errors.TextDownloadFailed

class BillTextDownloader[F[_]: Async](
  client: Client[F],
  config: BillTextPipelineConfig,
  logger: PipelineLogger[F],
) {

  private val StepName = "bill-text-download"

  def download(textUrl: String, textFormat: String, correlationId: UUID): F[String] = {
    val logCtx = LogContext(
      runId = correlationId.toString,
      stepName = StepName,
      correlationId = Some(correlationId),
      entityId = Some(textUrl),
    )

    for {
      uri <- Async[F].fromEither(
        Uri.fromString(textUrl).leftMap { err =>
          TextDownloadFailed(textUrl, textFormat, s"Invalid URL: ${err.sanitized}", err)
        }
      )
      _ <- logger.info(logCtx, s"Downloading bill text from $textUrl (format=$textFormat)")
      content <- executeDownload(uri, textUrl, textFormat)
      _ <- validateSize(content, textUrl, textFormat)
      extracted = extractText(content, textFormat)
      _ <- logger.info(logCtx, s"Downloaded ${extracted.length} characters from $textUrl")
    } yield extracted
  }

  private def executeDownload(uri: Uri, textUrl: String, textFormat: String): F[String] = {
    val timeout = config.downloadTimeoutSeconds.seconds

    Async[F].timeoutTo(
      fa = client.run(Request[F](uri = uri)).use { response =>
        response.status match {
          case Status.NotFound =>
            Async[F].raiseError[String](
              TextDownloadFailed(
                textUrl,
                textFormat,
                "HTTP 404 - bill text not found",
                new RuntimeException("404"),
              )
            )
          case status if status.isSuccess =>
            response.as[String]
          case status =>
            response.as[String].flatMap { body =>
              Async[F].raiseError[String](
                TextDownloadFailed(
                  textUrl,
                  textFormat,
                  s"HTTP ${status.code}: $body",
                  new RuntimeException(s"HTTP ${status.code}"),
                )
              )
            }
        }
      },
      duration = timeout,
      fallback = Async[F].raiseError(
        TextDownloadFailed(
          textUrl,
          textFormat,
          s"Download timed out after ${config.downloadTimeoutSeconds}s",
          new RuntimeException("timeout"),
        )
      ),
    )
  }

  private def validateSize(content: String, textUrl: String, textFormat: String): F[Unit] = {
    val sizeBytes = content.getBytes("UTF-8").length.toLong
    if (sizeBytes > config.maxContentBytes) {
      Async[F].raiseError(
        TextDownloadFailed(
          textUrl,
          textFormat,
          s"Content size $sizeBytes exceeds maximum ${config.maxContentBytes} bytes",
          new RuntimeException("size exceeded"),
        )
      )
    } else {
      Async[F].unit
    }
  }

  private[download] def extractText(content: String, textFormat: String): String = {
    textFormat match {
      case "Formatted Text" => stripHtml(content)
      case "Formatted XML"  => stripXml(content)
      case "PDF"            => content // PDF extraction would need PDFBox; for now pass through
      case _                => content
    }
  }

  private[download] def stripHtml(html: String): String = {
    html
      .replaceAll("<br\\s*/?>", "\n")
      .replaceAll("<p[^>]*>", "\n\n")
      .replaceAll("</p>", "")
      .replaceAll("<[^>]+>", "")
      .replaceAll("&amp;", "&")
      .replaceAll("&lt;", "<")
      .replaceAll("&gt;", ">")
      .replaceAll("&quot;", "\"")
      .replaceAll("&#167;", "\u00A7")
      .replaceAll("&nbsp;", " ")
      .replaceAll("(?m)^\\s+$", "")
      .replaceAll("\n{3,}", "\n\n")
      .trim
  }

  private[download] def stripXml(xml: String): String = {
    xml
      .replaceAll("<\\?[^?]*\\?>", "")
      .replaceAll("<!--[\\s\\S]*?-->", "")
      .replaceAll("<[^>]+>", " ")
      .replaceAll("\\s+", " ")
      .trim
  }

}
