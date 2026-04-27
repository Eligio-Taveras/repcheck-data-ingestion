package repcheck.ingestion.bills.text.extraction

import java.nio.file.Path

import cats.effect.Async
import cats.syntax.all._

import org.apache.pdfbox.Loader
import org.apache.pdfbox.text.PDFTextStripper
import repcheck.ingestion.bills.text.errors.PdfExtractionFailed

/**
 * PDFBox-backed text extractor for the `"PDF"` format. Replaces the prior
 * [[repcheck.ingestion.bills.text.download.BillTextDownloader.extractText]] PDF case which simply returned the raw
 * bytes coerced to a UTF-8 string — a real bug that put binary nonsense into `raw_bill_text.content` for every PDF
 * version.
 *
 * ==Memory profile==
 *
 * `PDFTextStripper.getText(document)` materializes the full extracted text as a single String. PDFBox itself parses
 * pages on demand from the underlying `RandomAccessRead` so the PDF page tree stays bounded; the returned String is the
 * only large in-heap allocation. For typical bill-text PDFs this is ≤ 4 MB of plain text even for 100+ page STATUTE
 * PDFs (the original PDF is dominated by font tables, layout metadata, and embedded fonts — extracted text is a
 * fraction of the source file size).
 *
 * Loaded via `Loader.loadPDF(File)` (PDFBox 3.x replaces the deprecated `PDDocument.load`) so the document is closed
 * automatically when the `try-with-resources` block exits — no `Resource[F, PDDocument]` lifecycle wiring needed at the
 * call site.
 *
 * @param path
 *   absolute path to the on-disk PDF written by [[repcheck.ingestion.bills.text.download.BillTextDownloader]] during
 *   the streaming-to-temp-file phase. Caller owns the file lifecycle (typically a `Resource[F, Path]` that auto-deletes
 *   on Resource release).
 *
 * @return
 *   the document's plain-text content as a single String. The string preserves logical reading order page-by-page;
 *   downstream whitespace normalization in [[BillTextExtractor.normalizeWhitespace]] collapses PDF's typical page-break
 *   runs of blank lines.
 */
object PDFTextExtractor {

  def extract[F[_]: Async](path: Path): F[String] =
    Async[F]
      .blocking {
        val document = Loader.loadPDF(path.toFile)
        try {
          val stripper = new PDFTextStripper
          // sortByPosition=false (default) preserves reading order as it appears in the PDF's page-content stream.
          // Bills are linear documents so the default is correct; only multi-column or complex-layout PDFs benefit
          // from sortByPosition=true (and that's slower).
          stripper.getText(document)
        } finally document.close()
      }
      .handleErrorWith { error =>
        Async[F].raiseError(
          PdfExtractionFailed(path.toString, error.getMessage, error)
        )
      }

}
