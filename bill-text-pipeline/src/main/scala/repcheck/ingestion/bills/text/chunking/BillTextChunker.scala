package repcheck.ingestion.bills.text.chunking

/**
 * Naive character-window chunker for bill text. Each invocation slices the input into consecutive substrings of length
 * `maxChunkChars`, with no overlap and no awareness of sentence / paragraph / SEC. boundaries — that's what the
 * structured-section pipeline (future component) is for. This chunker exists purely so the embedding model
 * (`qwen3-embedding`, 1536 dims) can produce one vector per slice without exceeding its input context window.
 *
 * The split happens by `String.length` (Java char count = UTF-16 code units). Bill text is overwhelmingly ASCII so the
 * char count tracks byte count closely; the model itself enforces the precise token boundary, so a slightly oversized
 * char window simply means the model truncates inside that slice. Configuring `maxChunkChars` well below the model's
 * limit gives headroom.
 *
 * Determinism + idempotency: chunking is a pure function of `(text, maxChunkChars)`. Repeated calls with the same
 * inputs produce the same `List[String]`, in the same order. The pipeline relies on this when re-processing a version —
 * `ORDER BY chunk_index` reconstructs the original document exactly.
 *
 * Contract: callers must pass `maxChunkChars > 0`. The pipeline-level processor validates this once at the entry point
 * (raising via `Async[F].raiseError` so the failure surfaces through the effect channel rather than a synchronous
 * throw); this object trusts the contract and short-circuits non-positive values to `Nil` defensively so even a
 * regression in the caller's validation doesn't produce an infinite loop on `Range.by(0)`.
 */
object BillTextChunker {

  /**
   * Split `text` into a list of consecutive substrings each at most `maxChunkChars` characters long.
   *
   *   - empty input returns `Nil` (no chunks for empty text — the upstream pipeline skips persistence);
   *   - non-positive `maxChunkChars` also returns `Nil` (defensive — see object scaladoc);
   *   - input shorter than `maxChunkChars` returns a single-element list containing the whole text;
   *   - otherwise returns `ceil(text.length / maxChunkChars)` substrings, the last of which may be shorter than
   *     `maxChunkChars`.
   */
  def chunk(text: String, maxChunkChars: Int): List[String] =
    if (text.isEmpty || maxChunkChars <= 0) {
      List.empty[String]
    } else if (text.length <= maxChunkChars) {
      List(text)
    } else {
      val len = text.length
      (0 until len by maxChunkChars).iterator
        .map(start => text.substring(start, math.min(start + maxChunkChars, len)))
        .toList
    }

}
