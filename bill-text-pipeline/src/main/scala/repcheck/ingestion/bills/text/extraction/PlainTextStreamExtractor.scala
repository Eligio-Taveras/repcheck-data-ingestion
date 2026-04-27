package repcheck.ingestion.bills.text.extraction

import java.nio.file.{Path => NioPath}

import cats.effect.Async

import fs2.io.file.{Files, Path => FsPath}
import fs2.{text, Stream}

/**
 * Streaming plain-text extractor. Reads the temp file as a UTF-8 byte stream, decodes to text fragments, normalizes
 * each fragment's whitespace, and emits the result.
 *
 * ==Streaming shape==
 *
 * `Files.readAll[F](path)` emits the file as `Stream[F, Byte]` in fs2's default chunk size (~64 KiB).
 * `text.utf8.decode` collects bytes into UTF-8-aligned text chunks (handling multi-byte boundaries correctly across
 * chunk edges). The resulting `Stream[F, String]` flows downstream where the chunker buffers fragments into model-sized
 * chunks.
 *
 * ==Whitespace handling==
 *
 * Each emitted fragment has internal whitespace runs collapsed (`\s+ → ` `) but is **not** trimmed — trimming
 * per-fragment would destroy whitespace at fragment boundaries (e.g. the newline between paragraphs straddling two fs2
 * chunks). Final trim happens at the chunker level, on each emitted fixed-size chunk, where it only affects ~1
 * character at each end of a 12 KB chunk.
 *
 * ==Heap profile==
 *
 * Heap usage during extraction is bounded by one fs2 chunk (~64 KiB encoded bytes → ~64K UTF-8 characters) plus the
 * downstream chunker's working buffer. No reference to the full file content is ever held.
 */
object PlainTextStreamExtractor {

  /**
   * Read the supplied path as UTF-8 plain text and emit a stream of normalized text fragments.
   *
   * @param path
   *   absolute path to the on-disk download. Must be UTF-8 encoded; non-UTF-8 input causes [[fs2.text.utf8.decode]] to
   *   surface a decoding error through the F effect's error channel.
   */
  def extract[F[_]: Async](path: NioPath): Stream[F, String] =
    Files
      .forAsync[F]
      .readAll(FsPath.fromNioPath(path))
      .through(text.utf8.decode)
      .map(BillTextExtractor.collapseWhitespace)
      .filter(_.nonEmpty)

}
