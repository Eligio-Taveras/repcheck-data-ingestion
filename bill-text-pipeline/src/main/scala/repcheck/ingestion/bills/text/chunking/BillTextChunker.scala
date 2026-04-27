package repcheck.ingestion.bills.text.chunking

import fs2.{Pipe, Pull, Stream}

/**
 * Streaming bill-text chunker. Takes a `Stream[F, String]` of semantic fragments emitted by the streaming extraction
 * layer (e.g. one paragraph from HTML, one `<section>` text from XML, one page from PDF) and emits a `Stream[F,
 * String]` of fixed-size chunks suitable for the embedding model's context window.
 *
 * ==Heap profile==
 *
 * Heap usage is bounded by `maxChunkChars + maxFragmentSize` regardless of total document size — incoming fragments are
 * accumulated into a buffer; whenever the buffer crosses `maxChunkChars` it's drained chunk-by-chunk; on stream
 * completion the residual (less than `maxChunkChars`) is emitted as the last chunk. The chunker itself never holds more
 * than one buffer + one fragment at a time, so a 1 GiB document produces the same heap footprint as a 10 KiB one.
 *
 * ==Naive character-window split==
 *
 * Splitting is by `String.length` (Java char count = UTF-16 code units), with no awareness of sentence / paragraph /
 * `SEC.` boundaries — that's what the structured-section pipeline (future component) is for. This chunker exists purely
 * so the embedding model (`bill-text-embedding`, currently backed by qwen3-embedding:0.6b, 1024 dims) can produce one
 * vector per slice without exceeding its input context window. Bill text is overwhelmingly ASCII so the char count
 * tracks byte/token count closely; the model truncates inside any slightly oversized slice on its own end.
 *
 * ==Determinism + ordering==
 *
 * For a fixed `(input fragments, maxChunkChars)` pair, [[chunkPipe]] emits the same chunks in the same order. The
 * concatenation of all emitted chunks equals the concatenation of all input fragments. Re-processing a bill yields
 * identical chunks → `ORDER BY chunk_index` after persistence reconstructs the document exactly.
 */
object BillTextChunker {

  /**
   * fs2 Pipe that converts a stream of text fragments into a stream of fixed-size chunks.
   *
   * Each emitted chunk is exactly `maxChunkChars` long, except the final chunk which may be shorter (whatever residual
   * is left in the buffer when upstream completes). Empty fragments are absorbed harmlessly — they simply don't grow
   * the buffer. An entirely empty input stream produces an empty output stream (no zero-length chunks emitted).
   *
   * @param maxChunkChars
   *   maximum chunk size in characters. Must be positive. Non-positive values yield an empty output stream defensively;
   *   pipeline-level callers validate this earlier and raise via the F effect channel
   *   ([[repcheck.ingestion.bills.text.embedding.InvalidChunkSize]]).
   */
  def chunkPipe[F[_]](maxChunkChars: Int): Pipe[F, String, String] = { in =>
    if (maxChunkChars <= 0) {
      Stream.empty
    } else {
      go(in, "", maxChunkChars).stream
    }
  }

  /**
   * Recursive `Pull` that pulls one fragment from upstream, appends it to the running buffer, drains every full chunk
   * out of the buffer, and recurses on the remaining (sub-`maxChunkChars`) tail. On upstream termination, emits the
   * final residual as the last chunk if non-empty.
   *
   * Pull-based rather than `evalMap`-based because each input fragment may produce zero, one, or many output chunks
   * (depending on how far over `maxChunkChars` the buffer grew). `evalMap` is 1:1; `Pull.output1` repeatedly inside a
   * recursive pull lets us emit any number of chunks per input.
   */
  private def go[F[_]](
    upstream: Stream[F, String],
    buffer: String,
    maxChunkChars: Int,
  ): Pull[F, String, Unit] =
    upstream.pull.uncons1.flatMap {
      case Some((fragment, rest)) =>
        drainFullChunks(buffer + fragment, maxChunkChars).flatMap(newBuffer => go(rest, newBuffer, maxChunkChars))
      case None =>
        val finalChunk = buffer.trim
        if (finalChunk.nonEmpty) Pull.output1(finalChunk) else Pull.done
    }

  /**
   * Repeatedly slice `maxChunkChars` off the front of `buffer` and emit it as a chunk (trimmed), until the buffer is
   * shorter than `maxChunkChars`. Returns the residual buffer (always strictly shorter than `maxChunkChars`).
   *
   * The per-chunk `.trim` compensates for the upstream extractors' decision to collapse whitespace runs without
   * trimming fragment-by-fragment (because trimming a fragment destroys inter-fragment whitespace at boundaries).
   * Trimming once per chunk only affects ~1 character at each end of a 12 KB chunk and gets the embedding model the
   * clean input it expects.
   *
   * If the trimmed chunk is empty (the slice was pure whitespace), it's omitted — the embedding model can't do anything
   * with empty input and `raw_bill_text.content` has a NOT NULL implicit contract.
   */
  private def drainFullChunks[F[_]](buffer: String, maxChunkChars: Int): Pull[F, String, String] =
    if (buffer.length < maxChunkChars) {
      Pull.pure(buffer)
    } else {
      val chunk     = buffer.substring(0, maxChunkChars).trim
      val remaining = buffer.substring(maxChunkChars)
      val emitChunk = if (chunk.nonEmpty) Pull.output1(chunk) else Pull.done
      emitChunk >> drainFullChunks(remaining, maxChunkChars)
    }

}
