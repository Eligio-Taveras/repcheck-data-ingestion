package repcheck.ingestion.bills.text.extraction

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}

import scala.xml.XML

import cats.effect.Async
import cats.syntax.all._

import org.jsoup.Jsoup

/**
 * Reads a downloaded bill text body from disk and extracts the plain prose for embedding + storage in `raw_bill_text`.
 *
 * The streaming-to-temp-file design keeps the raw HTTP body off-heap (the bytes spool through
 * `fs2.io.file.Files.writeAll` during download); the extractor opens the file, parses according to format, and returns
 * the extracted plain text as a single String. The String IS in heap, but for typical bill text (post-extraction prose,
 * after whitespace normalization) it's a fraction of the raw body — empirically 10–30% of the source file size for
 * HTML, smaller for XML, and even smaller for PDF where layout/font tables dominate the source bytes.
 *
 * ==Per-format dispatch==
 *
 * Mirrors the format-string matching that previously lived inside
 * [[repcheck.ingestion.bills.text.download.BillTextDownloader.extractText]]. The PDF case is now real (PDFBox-backed)
 * instead of the prior bug-prone "treat raw bytes as UTF-8 String" coerce. Other formats parse from disk:
 *
 *   - **`Formatted Text`** — Congress.gov serves an `<html><body><pre>...bill text...</pre></body></html>` shell; Jsoup
 *     parses the file, selects the `<pre>` element, returns its text content (HTML entities decoded).
 *   - **`Formatted XML`** — USLM-format XML; `scala.xml.XML.loadFile` parses the document and we descend into
 *     `<legis-body>` for the actual legislative content (skipping `<metadata>`, `<form>`, etc).
 *   - **`PDF`** — [[PDFTextExtractor]] streams text via PDFBox's `PDFTextStripper`.
 *   - **anything else** — read the file as UTF-8 plain text. Catch-all for `text/plain` and any future format we
 *     haven't taught the dispatcher about; produces correct (if unstructured) output.
 *
 * ==Whitespace==
 *
 * All formats run through [[normalizeWhitespace]] at the end — collapses runs of whitespace (spaces, tabs, newlines,
 * indentation) to single spaces and trims. Bills downloaded as Formatted Text arrive with `<pre>` whitespace preserved,
 * which is dead tokens for the embedding model; normalizing shrinks the input ~10–22% (measured on PLAW-119publ60.htm).
 * The trade-off is loss of original whitespace formatting in `raw_bill_text.content`; downstream consumers needing
 * high-fidelity display can refetch from `bill_text_versions.url`.
 */
object BillTextExtractor {

  /**
   * Extract bill text from a downloaded file based on the supplied `textFormat` string. Format strings come from
   * Congress.gov (`bill.textFormat` field) so this matches their case-sensitive labels exactly.
   *
   * @param path
   *   absolute path to the on-disk download. Caller owns the file lifecycle (typically wrapped in a `Resource[F, Path]`
   *   that auto-deletes on close).
   * @param textFormat
   *   the format label from Congress.gov: `"Formatted Text"`, `"Formatted XML"`, `"PDF"`, etc.
   * @return
   *   the extracted plain text, normalized for whitespace.
   */
  def extract[F[_]: Async](path: Path, textFormat: String): F[String] =
    textFormat match {
      case "Formatted Text" => extractHtml[F](path).map(normalizeWhitespace)
      case "Formatted XML"  => extractXml[F](path).map(normalizeWhitespace)
      case "PDF"            => PDFTextExtractor.extract[F](path).map(normalizeWhitespace)
      case _                => extractPlainText[F](path).map(normalizeWhitespace)
    }

  /**
   * Jsoup-based HTML extraction reading directly from the file (`Jsoup.parse(File, charset)`). For the typical
   * Congress.gov "Formatted Text" payload we want the contents of the `<pre>` element; if the document doesn't have one
   * we fall back to the body text so an unexpected layout doesn't return empty.
   */
  private def extractHtml[F[_]: Async](path: Path): F[String] =
    Async[F].blocking {
      val doc         = Jsoup.parse(path.toFile, StandardCharsets.UTF_8.name())
      val preElements = doc.select("pre")
      if (preElements.isEmpty) {
        doc.body().text()
      } else {
        preElements.text()
      }
    }

  /**
   * scala-xml extraction reading directly from the file. Descends into `<legis-body>` for the legislative content; if
   * that node is absent (older XML, unexpected schema) returns the whole document text as a fallback.
   *
   * NOTE: scala-xml's `XML.loadFile` builds an in-memory tree. SAX-based streaming is theoretically lighter on heap but
   * the additional complexity isn't worth it given the post-streaming-download memory profile already targets the Jsoup
   * DOM size as the dominant peak. If a future bill XML grows past current sizes we can revisit with a SAX-based
   * ContentHandler.
   */
  private def extractXml[F[_]: Async](path: Path): F[String] =
    Async[F].blocking {
      val xml       = XML.loadFile(path.toFile)
      val legisBody = xml \\ "legis-body"
      if (legisBody.isEmpty) {
        xml.text
      } else {
        legisBody.text
      }
    }

  /**
   * Read the entire file as a UTF-8 String. Used for the `text/plain` catch-all branch and any unknown format. Loads
   * fully into heap; bounded by the configured `pipeline.max-content-bytes` ceiling at the download phase.
   */
  private def extractPlainText[F[_]: Async](path: Path): F[String] =
    Async[F].blocking(Files.readString(path, StandardCharsets.UTF_8))

  /**
   * Collapse runs of whitespace (spaces, tabs, newlines, indentation) to single spaces and trim. Public so test specs
   * can verify the normalization contract independent of the format-dispatch layer.
   */
  private[extraction] def normalizeWhitespace(text: String): String =
    text.replaceAll("\\s+", " ").trim

}
