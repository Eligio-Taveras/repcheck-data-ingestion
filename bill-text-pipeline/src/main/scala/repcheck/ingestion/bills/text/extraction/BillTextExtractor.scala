package repcheck.ingestion.bills.text.extraction

import java.nio.file.Path

import cats.effect.Async

import fs2.Stream

/**
 * Streaming dispatcher for bill-text extraction. Reads a downloaded bill text body from disk and emits the extracted
 * prose as a `Stream[F, String]` — one fragment per natural unit of the source format (one PDF page, one HTML
 * `characters` event, one XML `CHARACTERS` event, one fs2 byte-decoded chunk for plain text).
 *
 * ==Why streaming the extractor==
 *
 * Phase 2 of `bill-text-10mb-streaming.md` made the *download* phase streaming: bytes spool from HTTP into a temp file
 * in fs2 chunks, never accumulating in heap. But the next stage — extraction — still buffered the whole document via
 * Jsoup DOM / scala-xml DOM / `PDFTextStripper.getText(document)` (a single String for the entire PDF). For a 75 MiB
 * STATUTE PDF that meant ~75 MiB of in-heap String + Jsoup's DOM (5–10× the source) before anything else could run.
 * Phase 3 closes that gap: each format has a streaming extractor (TagSoup SAX for HTML, StAX for XML, PDFBox per-page
 * for PDF, fs2 byte decoding for plain text), and downstream chunking + embedding + INSERT runs in the same fs2 stream
 * so backpressure flows end-to-end.
 *
 * ==Per-format dispatch==
 *
 *   - **`Formatted Text`** — [[HtmlStreamExtractor]] (TagSoup SAX, push-pull bridged via a bounded queue).
 *   - **`Formatted XML`** — [[XmlStreamExtractor]] (JDK StAX, `XMLStreamReader` event walk).
 *   - **`PDF`** — [[PdfStreamExtractor]] (PDFBox + `RandomAccessReadBufferedFile` + per-page `PDFTextStripper`).
 *   - **anything else** — [[PlainTextStreamExtractor]] (`fs2.Files.readAll` + `text.utf8.decode`). Catch-all for
 *     `text/plain` and any unknown format.
 *
 * ==Whitespace contract==
 *
 * Each emitted fragment has internal whitespace runs collapsed (`\s+ → ` `) but is **not** trimmed. Per-fragment
 * trimming would destroy whitespace at fragment boundaries (the newline between paragraphs that happens to straddle two
 * fs2 byte chunks, or the space between two PDF page texts). Final trimming happens at the chunker level on each
 * emitted fixed-size chunk.
 */
object BillTextExtractor {

  /**
   * Extract bill text from a downloaded file as a stream of normalized text fragments. Format strings come from
   * Congress.gov (`bill.textFormat` field) so this matches their case-sensitive labels exactly.
   *
   * @param path
   *   absolute path to the on-disk download. Caller owns the file lifecycle (typically wrapped in a `Resource[F, Path]`
   *   that auto-deletes on close).
   * @param textFormat
   *   the format label from Congress.gov: `"Formatted Text"`, `"Formatted XML"`, `"PDF"`, etc.
   */
  def extractStream[F[_]: Async](path: Path, textFormat: String): Stream[F, String] =
    textFormat match {
      case "Formatted Text" => HtmlStreamExtractor.extract[F](path)
      case "Formatted XML"  => XmlStreamExtractor.extract[F](path)
      case "PDF"            => PdfStreamExtractor.extract[F](path)
      case _                => PlainTextStreamExtractor.extract[F](path)
    }

  /**
   * Collapse runs of whitespace (spaces, tabs, newlines, indentation) to single spaces. Crucially does **not** trim,
   * because the streaming extractors call this per-fragment and trimming destroys inter-fragment whitespace at
   * boundaries. The chunker applies the final `.trim` once on each emitted fixed-size chunk; see [[BillTextChunker]].
   *
   * Public so test specs and per-format extractors can verify / use the normalization contract independent of the
   * dispatch layer.
   */
  private[extraction] def collapseWhitespace(text: String): String =
    text.replaceAll("\\s+", " ")

}
