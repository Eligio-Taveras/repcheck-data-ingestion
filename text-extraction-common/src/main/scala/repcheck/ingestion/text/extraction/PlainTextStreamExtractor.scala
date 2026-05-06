package repcheck.ingestion.text.extraction

import fs2.Stream

/**
 * Streaming plain-text extractor. Decodes a `Stream[F, Byte]` (typically the HTTP response body) as UTF-8 text and
 * emits a stream of normalized text fragments.
 *
 * ==Streaming shape==
 *
 * `fs2.text.utf8.decode` collects bytes into UTF-8-aligned text chunks (handling multi-byte boundaries correctly across
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
 * downstream chunker's working buffer. No reference to the full body is ever held.
 */
object PlainTextStreamExtractor {

  /**
   * Decode the supplied byte stream as UTF-8 plain text and emit a stream of normalized text fragments.
   *
   * @param bytes
   *   the byte stream to decode. Must be UTF-8 encoded; non-UTF-8 input causes `fs2.text.utf8.decode` to surface a
   *   decoding error through the F effect's error channel.
   */
  def extract[F[_]](bytes: Stream[F, Byte]): Stream[F, String] =
    bytes
      .through(fs2.text.utf8.decode)
      .map(TextExtractor.collapseWhitespace)
      .filter(_.nonEmpty)

}
