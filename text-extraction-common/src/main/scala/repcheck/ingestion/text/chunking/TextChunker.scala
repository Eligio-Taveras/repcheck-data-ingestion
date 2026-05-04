package repcheck.ingestion.text.chunking

import scala.annotation.tailrec

import cats.effect.kernel.Async

import fs2.{Chunk, Pipe, Pull, Stream}

/**
 * Streaming text chunker. Takes a `Stream[F, String]` of semantic fragments emitted by the streaming extraction layer
 * (e.g. one paragraph from HTML, one `<section>` text from XML, one page from PDF) and emits a `Stream[F, String]` of
 * fixed-size chunks suitable for the embedding model's context window.
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
 * ==Stack safety==
 *
 *   - The inner buffer-draining loop ([[collectFullChunksFromBuffer]]) is a classic accumulator-based `@tailrec` —
 *     verified by the compiler — so it can flush a 1 GiB buffer worth of full chunks without growing the JVM stack.
 *   - The outer fragment-pulling loop ([[pullFragmentsAndEmitChunks]]) uses fs2's `Pull.loop` primitive, which is
 *     stack-safe by construction (fs2's Pull monad is a stack-safe free interpretation, and `Pull.loop` is the
 *     library-blessed way to express "iterate this step until it returns `None`"). Both `Pull.loop` and `@tailrec` are
 *     explicit-by-construction stack-safety mechanisms; nothing here relies on incidental tail-call elimination by the
 *     JVM.
 *
 * ==Determinism + ordering==
 *
 * For a fixed `(input fragments, maxChunkChars)` pair, [[chunkPipe]] emits the same chunks in the same order. The
 * concatenation of all emitted chunks equals the concatenation of all input fragments (modulo whitespace trim at chunk
 * boundaries). Re-processing a document yields identical chunks → `ORDER BY chunk_index` after persistence reconstructs
 * the document.
 */
object TextChunker {

  /**
   * fs2 Pipe that converts a stream of text fragments into a stream of fixed-size chunks.
   *
   * Each emitted chunk is exactly `maxChunkChars` long, except the final chunk which may be shorter (whatever residual
   * is left in the buffer when upstream completes). Empty fragments are absorbed harmlessly — they simply don't grow
   * the buffer. An entirely empty input stream produces an empty output stream (no zero-length chunks emitted).
   *
   * @param maxChunkChars
   *   maximum chunk size in characters. Must be `> 0`. Non-positive values raise [[InvalidChunkSize]] through the F
   *   effect channel as soon as the pipe is consumed — the chunker fails the run loud rather than silently producing
   *   zero chunks for non-empty input.
   */
  def chunkPipe[F[_]: Async](maxChunkChars: Int): Pipe[F, String, String] = { in =>
    if (maxChunkChars <= 0) {
      Stream.raiseError[F](InvalidChunkSize(maxChunkChars))
    } else {
      pullFragmentsAndEmitChunks[F](maxChunkChars).apply((in, "")).stream
    }
  }

  /**
   * Outer loop, expressed via fs2's stack-safe `Pull.loop` primitive. Each iteration pulls one fragment from upstream,
   * appends it to the buffer, drains every full chunk via the `@tailrec` inner accumulator, emits all the collected
   * chunks at once, and iterates with the residual buffer. On upstream termination, emits the final residual chunk if
   * non-empty.
   *
   * `Pull.loop` returns `R => Pull[F, O, Unit]` — partially-applied here so the call site reads
   * `pullFragmentsAndEmitChunks(maxChunkChars).apply((upstream, ""))`.
   */
  private def pullFragmentsAndEmitChunks[F[_]](
    maxChunkChars: Int
  ): ((Stream[F, String], String)) => Pull[F, String, Unit] =
    Pull.loop[F, String, (Stream[F, String], String)] {
      case (upstream, buffer) =>
        upstream.pull.uncons1.flatMap {
          case Some((fragment, rest)) =>
            val (chunksToEmit, residualBuffer) = collectFullChunksFromBuffer(buffer + fragment, maxChunkChars, Nil)
            Pull.output(Chunk.from(chunksToEmit)).as(Some((rest, residualBuffer)))
          case None =>
            val finalChunk = buffer.trim
            val emit       = if (finalChunk.nonEmpty) Pull.output1(finalChunk) else Pull.done
            emit.as(None)
        }
    }

  /**
   * `@tailrec` accumulator that slices `maxChunkChars` off the front of `buffer` repeatedly, collecting trimmed
   * non-empty chunks into `acc` (in reverse order), until the buffer is shorter than `maxChunkChars`. Returns the
   * collected chunks in document order along with the residual buffer (always strictly shorter than `maxChunkChars`).
   *
   * The per-chunk `.trim` compensates for the upstream extractors' decision to collapse whitespace runs without
   * trimming fragment-by-fragment (because trimming a fragment destroys inter-fragment whitespace at boundaries).
   * Trimming once per chunk only affects ~1 character at each end of a 12 KB chunk and gets the embedding model the
   * clean input it expects.
   *
   * Empty post-trim chunks are dropped — the embedding model can't do anything with empty input and
   * `raw_bill_text.content` has a NOT NULL implicit contract.
   */
  @tailrec
  private def collectFullChunksFromBuffer(
    buffer: String,
    maxChunkChars: Int,
    acc: List[String],
  ): (List[String], String) =
    if (buffer.length < maxChunkChars) {
      (acc.reverse, buffer)
    } else {
      val chunk     = buffer.substring(0, maxChunkChars).trim
      val remaining = buffer.substring(maxChunkChars)
      val newAcc    = if (chunk.nonEmpty) chunk :: acc else acc
      collectFullChunksFromBuffer(remaining, maxChunkChars, newAcc)
    }

}
