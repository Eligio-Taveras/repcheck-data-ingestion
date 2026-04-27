package repcheck.ingestion.bills.text.errors

/**
 * Raised when [[repcheck.ingestion.bills.text.extraction.PDFTextExtractor]] fails to parse a downloaded PDF — typically
 * because the file is corrupted, password-protected, or PDFBox encounters an unsupported feature inside the document.
 *
 * Classified as Systemic by [[repcheck.ingestion.bills.text.pipeline.BillTextProcessor.classifyError]] (catch-all
 * branch): retrying the same URL produces the same bytes which fail the same way. Operator action is to inspect the PDF
 * manually, raise an issue with Congress.gov if the source is malformed, or accept that this version simply doesn't
 * have extractable text.
 *
 * @param path
 *   the on-disk path PDFBox was reading. Typically a temp file created by the streaming download phase; the path string
 *   is logged for debugging but the file is auto-deleted by the enclosing `Resource[F, Path]` after this exception
 *   propagates.
 * @param reason
 *   PDFBox's underlying error message (e.g. `"Document is encrypted"`, `"Cross-reference table is invalid"`).
 * @param cause
 *   the original PDFBox / IO exception. Preserved so the cause-chain classifier in
 *   [[repcheck.ingestion.bills.text.errors.BillSummariesApiErrorClassifier]]-style retries can introspect downstream.
 */
final case class PdfExtractionFailed(
  path: String,
  reason: String,
  cause: Throwable,
) extends Exception(s"PDF extraction failed for $path: $reason", cause)
