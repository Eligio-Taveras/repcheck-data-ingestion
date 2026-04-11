package com.repcheck.bills.text.download

import java.util.UUID

import scala.concurrent.duration._
import scala.xml.XML

import cats.effect.Async
import cats.syntax.all._

import org.http4s.client.Client
import org.http4s.{Request, Status, Uri}

import org.jsoup.Jsoup
import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}

import com.repcheck.bills.text.config.BillTextPipelineConfig
import com.repcheck.bills.text.errors.{InvalidTextUrl, TextContentTooLarge, TextDownloadFailed, TextDownloadTimedOut}

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
      uri       <- parseUrl(textUrl)
      _         <- logger.info(logCtx, s"Downloading bill text from $textUrl (format=$textFormat)")
      content   <- executeDownload(uri, textUrl, textFormat)
      _         <- validateSize(content, textUrl)
      extracted <- extractText(content, textFormat)
      _         <- logger.info(logCtx, s"Downloaded ${extracted.length} characters from $textUrl")
    } yield extracted
  }

  private[download] def parseUrl(textUrl: String): F[Uri] =
    Async[F].fromEither(
      Uri.fromString(textUrl).leftMap(err => InvalidTextUrl(textUrl, err.sanitized))
    )

  private[download] def executeDownload(uri: Uri, textUrl: String, textFormat: String): F[String] = {
    val timeout = config.downloadTimeoutSeconds.seconds

    Async[F].timeoutTo(
      fa = client.run(Request[F](uri = uri)).use { response =>
        response.status match {
          case Status.NotFound =>
            Async[F].raiseError[String](
              TextDownloadFailed(textUrl, textFormat, "HTTP 404 - bill text not found")
            )
          case status if status.isSuccess =>
            response.as[String]
          case status =>
            response.as[String].flatMap { body =>
              Async[F].raiseError[String](
                TextDownloadFailed(textUrl, textFormat, s"HTTP ${status.code}: $body")
              )
            }
        }
      },
      duration = timeout,
      fallback = Async[F].raiseError(
        TextDownloadTimedOut(textUrl, config.downloadTimeoutSeconds)
      ),
    )
  }

  private[download] def validateSize(content: String, textUrl: String): F[Unit] = {
    val sizeBytes = content.getBytes("UTF-8").length.toLong
    if (sizeBytes > config.maxContentBytes) {
      Async[F].raiseError(
        TextContentTooLarge(textUrl, sizeBytes, config.maxContentBytes)
      )
    } else {
      Async[F].unit
    }
  }

  private[download] def extractText(content: String, textFormat: String): F[String] =
    Async[F].delay {
      textFormat match {
        case "Formatted Text" => extractHtmlText(content)
        case "Formatted XML"  => extractXmlText(content)
        case "PDF"            => content
        case _                => content
      }
    }

  /**
   * Extract bill text from Congress.gov "Formatted Text" HTML.
   *
   * The HTML structure is: `<html><body><pre>...bill text...</pre></body></html>` The `<pre>` tag contains the actual
   * bill text as pre-formatted plain text with HTML entities encoded. Jsoup parses the HTML and extracts the text
   * content from the `<pre>` element, decoding entities automatically.
   */
  private[download] def extractHtmlText(html: String): String = {
    val doc         = Jsoup.parse(html)
    val preElements = doc.select("pre")
    if (preElements.isEmpty) {
      doc.body().text()
    } else {
      preElements.text()
    }
  }

  /**
   * Extract bill text from Congress.gov "Formatted XML" (USLM format).
   *
   * The XML follows the United States Legislative Markup schema with key elements:
   *   - `<legis-body>`: contains the actual legislative text (sections, subsections, paragraphs)
   *   - `<metadata>` / `<dublinCore>`: document metadata (title, date, publisher) — excluded
   *   - `<form>`: header/intro block (congress, session, sponsors) — excluded
   *
   * We extract text only from `<legis-body>` to get the actual bill content without metadata or header boilerplate.
   */
  private[download] def extractXmlText(xmlContent: String): String = {
    val xml       = XML.loadString(xmlContent)
    val legisBody = xml \\ "legis-body"
    if (legisBody.isEmpty) {
      xml.text.replaceAll("\\s+", " ").trim
    } else {
      legisBody.text.replaceAll("\\s+", " ").trim
    }
  }

}
