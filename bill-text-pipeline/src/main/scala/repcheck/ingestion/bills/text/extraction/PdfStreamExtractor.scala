package repcheck.ingestion.bills.text.extraction

import java.nio.file.{Path => NioPath}

import cats.effect.{Async, Resource}
import cats.syntax.all._

import fs2.Stream

import org.apache.pdfbox.Loader
import org.apache.pdfbox.io.RandomAccessReadBufferedFile
import org.apache.pdfbox.pdmodel.PDDocument
import org.apache.pdfbox.text.PDFTextStripper
import repcheck.ingestion.bills.text.errors.PdfExtractionFailed

/**
 * Streaming PDF text extractor — emits one fragment per PDF page rather than buffering the whole document's text into a
 * single String.
 *
 * ==Why per-page is the closest streaming model PDFBox supports==
 *
 * PDF format requires random access by design — the cross-reference (xref) table at the end of the file maps object
 * numbers to byte offsets, and the parser seeks to those offsets as it walks. A truly byte-streaming PDF parser would
 * need a fundamentally different format. PDFBox supports two mitigations that together approximate streaming for our
 * purposes:
 *
 *   1. `RandomAccessReadBufferedFile` — opens the PDF as a `FileChannel`-backed random-access reader rather than
 *      memory-mapping or fully buffering. Heap usage during parsing is bounded by PDFBox's working window (typically a
 *      few hundred KB) rather than the document size.
 *   1. Per-page text extraction via `PDFTextStripper.processPage(page)` — instead of `getText(document)` (which buffers
 *      all pages' text into one String), we drive the stripper one page at a time and emit each page's text as a
 *      separate fragment. The chunker downstream concatenates and re-slices into model-sized chunks.
 *
 * Result: peak heap during PDF processing of any size document ≈ `(PDFBox parser working window) + (one page's
 * extracted text)`. For Congress.gov STATUTE PDFs that's a few hundred KB.
 *
 * ==Disk profile==
 *
 * No additional disk usage beyond the temp file already written by
 * [[repcheck.ingestion.bills.text.download.BillTextDownloader]] — `RandomAccessReadBufferedFile` reads from the
 * existing file via `FileChannel`. We don't enable PDFBox's separate scratch-file mode because random-access reading
 * from the temp file is sufficient for the streaming we need.
 *
 * @param path
 *   absolute path to the on-disk PDF written by the downloader. Caller owns the file lifetime.
 */
object PdfStreamExtractor {

  /**
   * Open the PDF, walk its pages, emit each page's text content as a fragment. The `PDDocument` is wrapped in a
   * `Resource` so it closes deterministically when the stream completes or fails. Per-page extraction runs inside
   * `Async[F].blocking` — PDFBox is synchronous and CPU-bound on parsing, so we want a blocking thread, not a
   * compute-pool thread.
   *
   * Errors during open or per-page strip surface as [[PdfExtractionFailed]], preserving the path string for log
   * debugging.
   */
  def extract[F[_]: Async](path: NioPath): Stream[F, String] =
    Stream
      .resource(documentResource[F](path))
      .flatMap { document =>
        val pageCount = document.getNumberOfPages
        Stream
          .range(0, pageCount)
          .evalMap { pageIndex =>
            Async[F]
              .blocking {
                val stripper = new PDFTextStripper
                // Pages are 1-indexed in PDFBox's stripper API.
                stripper.setStartPage(pageIndex + 1)
                stripper.setEndPage(pageIndex + 1)
                stripper.getText(document)
              }
              .map(BillTextExtractor.collapseWhitespace)
          }
          .filter(_.nonEmpty)
      }
      .handleErrorWith { error =>
        // Single uniform wrap so callers always see PdfExtractionFailed with the path baked in.
        // documentResource also wraps load-time errors; the slight redundancy on that path
        // (PdfExtractionFailed nested as cause of another PdfExtractionFailed) is preferable to
        // a branch arm that's unreachable in practice (forging a non-PdfExtractionFailed
        // mid-page error would require constructing a partially-corrupt PDF, which is more
        // ceremony than the diagnostic clarity it would buy).
        Stream.raiseError[F](PdfExtractionFailed(path.toString, error.getMessage, error))
      }

  /**
   * `Resource[F, PDDocument]` that opens a random-access reader on the supplied path, hands the document to the
   * Resource consumer, and closes the document (and its underlying reader) when the Resource releases. Errors during
   * `Loader.loadPDF` are surfaced as [[PdfExtractionFailed]] so the caller's log context still has the path.
   *
   * Resource composition: the reader is allocated as its own `Resource` so its release runs even if `loadPDF` fails
   * during acquisition. PDFBox's `RandomAccessReadBufferedFile.close()` is idempotent — closing it after PDFBox's
   * `PDDocument.close()` already closed it on the success path is safe.
   */
  private def documentResource[F[_]: Async](path: NioPath): Resource[F, PDDocument] = {
    val readerR: Resource[F, RandomAccessReadBufferedFile] = Resource.make(
      Async[F].blocking(new RandomAccessReadBufferedFile(path.toFile))
    )(reader => Async[F].blocking(reader.close()))

    readerR.flatMap { reader =>
      Resource.make(
        Async[F]
          .blocking(Loader.loadPDF(reader))
          .handleErrorWith(error =>
            Async[F].raiseError[PDDocument](PdfExtractionFailed(path.toString, error.getMessage, error))
          )
      )(document => Async[F].blocking(document.close()))
    }
  }

}
