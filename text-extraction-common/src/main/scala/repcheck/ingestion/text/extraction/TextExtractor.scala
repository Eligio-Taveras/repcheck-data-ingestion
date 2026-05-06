package repcheck.ingestion.text.extraction

import cats.effect.Async

import fs2.Stream

/**
 * Streaming dispatcher for format-agnostic text extraction. Consumes a `Stream[F, Byte]` (typically the HTTP response
 * body of a bill / amendment / record source) and emits the extracted prose as a `Stream[F, String]` — one fragment per
 * natural unit of the source format (one PDF page, one HTML `characters` event, one XML `CHARACTERS` event, one fs2
 * byte-decoded chunk for plain text).
 *
 * ==No temp file for HTML / XML / Plaintext==
 *
 * The HTML, XML, and plaintext extractors stream the bytes directly through their respective parsers — TagSoup's SAX
 * `parse(InputSource)`, StAX's `XMLStreamReader`, and `fs2.text.utf8.decode` all consume from an InputStream / byte
 * stream. No disk I/O. Heap stays bounded by the parser's working window (a few KB) regardless of body size.
 *
 * The PDF extractor is the single exception: PDF format requires random access (xref table at the end of the file), so
 * the bytes spool to a temp file inside [[PdfStreamExtractor]] before page-by-page extraction. That temp-file lifecycle
 * is fully internal to PdfStreamExtractor; the dispatcher's API is uniform across formats.
 *
 * ==Per-format dispatch==
 *
 *   - **`Formatted Text`** — [[BillsHtmlStreamExtractor]] (TagSoup SAX, push-pull bridged via a bounded queue).
 *   - **`Formatted XML`** — [[XmlStreamExtractor]] (JDK StAX, `XMLStreamReader` event walk).
 *   - **`PDF`** — [[PdfStreamExtractor]] (spool to temp file, then PDFBox per-page `PDFTextStripper`).
 *   - **anything else** — [[PlainTextStreamExtractor]] (`fs2.text.utf8.decode`). Catch-all for `text/plain` and any
 *     unknown format.
 *
 * ==Whitespace contract==
 *
 * Each emitted fragment has internal whitespace runs collapsed (`\s+ → ` `) but is **not** trimmed. Per-fragment
 * trimming would destroy whitespace at fragment boundaries (the newline between paragraphs that happens to straddle two
 * fs2 byte chunks, or the space between two PDF page texts). Final trimming happens at the chunker level on each
 * emitted fixed-size chunk.
 */
object TextExtractor {

  /**
   * Extract text from an upstream byte stream as a stream of normalized text fragments. Format strings come from
   * Congress.gov (`bill.textFormat` field) so this matches their case-sensitive labels exactly.
   *
   * @param bytes
   *   the byte stream to extract from. Backpressure flows up through this stream — bytes are pulled at the rate
   *   downstream stages can consume.
   * @param textFormat
   *   the format label from Congress.gov: `"Formatted Text"`, `"Formatted XML"`, `"PDF"`, etc.
   */
  def extractStream[F[_]: Async](bytes: Stream[F, Byte], textFormat: String): Stream[F, String] =
    textFormat match {
      case "Formatted Text" => BillsHtmlStreamExtractor.extract[F](bytes)
      case "Formatted XML"  => XmlStreamExtractor.extract[F](bytes)
      case "PDF"            => PdfStreamExtractor.extract[F](bytes)
      case _                => PlainTextStreamExtractor.extract[F](bytes)
    }

  /**
   * Collapse runs of whitespace (spaces, tabs, newlines, indentation) to single spaces. Crucially does **not** trim,
   * because the streaming extractors call this per-fragment and trimming destroys inter-fragment whitespace at
   * boundaries. The chunker applies the final `.trim` once on each emitted fixed-size chunk; see
   * [[repcheck.ingestion.text.chunking.TextChunker]].
   *
   * Public so test specs and per-format extractors can verify / use the normalization contract independent of the
   * dispatch layer.
   */
  private[text] def collapseWhitespace(text: String): String =
    text.replaceAll("\\s+", " ")

}
