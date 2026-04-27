package repcheck.ingestion.bills.text.extraction

import cats.effect.{Async, Resource}
import cats.syntax.all._

import fs2.Stream
import fs2.io.file.{Files, Flags, Path => FsPath}

import org.apache.pdfbox.Loader
import org.apache.pdfbox.io.RandomAccessReadBufferedFile
import org.apache.pdfbox.pdmodel.PDDocument
import org.apache.pdfbox.text.PDFTextStripper
import repcheck.ingestion.bills.text.errors.PdfExtractionFailed

/**
 * Streaming PDF text extractor. Writes the upstream byte stream to a temp file first (PDF requires random access to the
 * cross-reference table at the end of the file — there's no way to parse a PDF without it), then walks the document
 * page-by-page emitting one text fragment per page.
 *
 * ==Why per-page is the closest streaming model PDFBox supports==
 *
 * PDF format requires random access by design — the xref table at the end of the file maps object numbers to byte
 * offsets, and the parser seeks to those offsets as it walks. Truly byte-streaming PDF parsing isn't possible. The
 * mitigations:
 *
 *   1. **Spool to disk in fs2 chunks.** `fs2.Files.writeAll` writes the upstream byte stream to a temp file in ~64 KiB
 *      chunks. Heap during the spool phase is bounded by the fs2 chunk size, not the PDF size.
 *   1. **`RandomAccessReadBufferedFile`** opens the temp file via `FileChannel` rather than mmap or full buffer. Heap
 *      during parsing is bounded by PDFBox's working window (typically a few hundred KB).
 *   1. **Per-page text extraction** via `PDFTextStripper.setStartPage/setEndPage(p)` — instead of `getText(document)`
 *      (which buffers all pages' text into one String), drive the stripper one page at a time and emit each page's text
 *      as a separate fragment. The chunker downstream concatenates and re-slices into model-sized chunks.
 *
 * Result: peak heap for any PDF size ≈ `(fs2 spool chunk, ~64 KiB) + (PDFBox parser working window, ~hundreds of KB) +
 * (one page's extracted text)`. For Congress.gov STATUTE PDFs that's well under 1 MB.
 *
 * ==Why temp file (and not just an InputStream)==
 *
 * PDF format requires `seek` operations on the underlying read; `RandomAccessRead` is PDFBox's abstraction for that.
 * `RandomAccessReadBuffer` (the in-memory variant) would buffer the whole PDF in heap — defeating the streaming goal.
 * `RandomAccessReadBufferedFile` is file-backed and seekable; that's what we want. So PDF is the one format where we DO
 * need a temp file as an intermediate, but the temp file lifecycle is fully internal to this extractor — the caller
 * just hands us a `Stream[F, Byte]`.
 *
 * @param bytes
 *   the byte stream to parse. Caller (typically [[BillTextExtractor.extractStream]] dispatching by format) doesn't know
 *   about the temp-file dance.
 */
object PdfStreamExtractor {

  /**
   * Spool the byte stream to a temp file, open the PDF, walk its pages, emit each page's text content as a fragment.
   * The temp file is held in a `Resource` so it's deleted when the stream completes (success or error). The
   * `PDDocument` is similarly Resource-wrapped.
   *
   * Errors during spool, open, or per-page strip surface as [[PdfExtractionFailed]].
   */
  def extract[F[_]: Async](bytes: Stream[F, Byte]): Stream[F, String] =
    Stream
      .resource(spoolToTempFile[F](bytes))
      .flatMap { tempPath =>
        Stream
          .resource(documentResource[F](tempPath))
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
      }
      .handleErrorWith { error =>
        // Single uniform wrap so callers always see PdfExtractionFailed. documentResource also
        // wraps load-time errors; the slight redundancy on that path (PdfExtractionFailed nested
        // as cause of another PdfExtractionFailed) is preferable to a branch arm that's
        // unreachable in practice.
        Stream.raiseError[F](PdfExtractionFailed("<temp-file>", error.getMessage, error))
      }

  /**
   * Resource that creates a temp file, spools the upstream byte stream into it, and yields the file path. The temp file
   * is auto-deleted when the Resource releases.
   */
  private def spoolToTempFile[F[_]: Async](bytes: Stream[F, Byte]): Resource[F, FsPath] =
    Files
      .forAsync[F]
      .tempFile(dir = None, prefix = "bill-text-pdf-", suffix = ".bin", permissions = None)
      .evalTap(tempPath => bytes.through(Files.forAsync[F].writeAll(tempPath, Flags.Write)).compile.drain)

  /**
   * `Resource[F, PDDocument]` that opens a random-access reader on the supplied path, hands the document to the
   * Resource consumer, and closes the document (and its underlying reader) when the Resource releases. Errors during
   * `Loader.loadPDF` are surfaced as [[PdfExtractionFailed]] so the caller's log context still has the path.
   *
   * Resource composition: the reader is allocated as its own `Resource` so its release runs even if `loadPDF` fails
   * during acquisition. PDFBox's `RandomAccessReadBufferedFile.close()` is idempotent — closing it after PDFBox's
   * `PDDocument.close()` already closed it on the success path is safe.
   */
  private def documentResource[F[_]: Async](path: FsPath): Resource[F, PDDocument] = {
    val readerR: Resource[F, RandomAccessReadBufferedFile] = Resource.make(
      Async[F].blocking(new RandomAccessReadBufferedFile(path.toNioPath.toFile))
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
