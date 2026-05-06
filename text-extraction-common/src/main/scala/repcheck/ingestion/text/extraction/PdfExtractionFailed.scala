package repcheck.ingestion.text.extraction

/**
 * Raised when [[PdfStreamExtractor]] fails to parse a PDF — typically because the file is corrupted,
 * password-protected, or PDFBox encounters an unsupported feature inside the document.
 *
 * Pipelines consuming this exception should classify it as Systemic (retrying the same bytes produces the same
 * failure). Operator action is to inspect the PDF manually or accept that this version simply doesn't have extractable
 * text.
 *
 * @param path
 *   the on-disk path PDFBox was reading. Typically a temp file created by the streaming download phase.
 * @param reason
 *   PDFBox's underlying error message (e.g. `"Document is encrypted"`, `"Cross-reference table is invalid"`).
 * @param cause
 *   the original PDFBox / IO exception. Preserved so cause-chain classifiers can introspect downstream.
 */
final case class PdfExtractionFailed(
  path: String,
  reason: String,
  cause: Throwable,
) extends Exception(s"PDF extraction failed for $path: $reason", cause)
