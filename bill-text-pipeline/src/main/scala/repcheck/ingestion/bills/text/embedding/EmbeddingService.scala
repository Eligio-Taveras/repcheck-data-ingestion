package repcheck.ingestion.bills.text.embedding

trait EmbeddingService[F[_]] {

  /**
   * Generate a vector embedding for a single text.
   *
   * Convenience wrapper over [[generateEmbeddings]] for the single-input case. Implementations should produce a
   * fixed-dimensional embedding vector suitable for semantic similarity search; the underlying call shape is whatever
   * the backing model expects (Ollama's `/api/embed` endpoint accepts both single and array forms).
   *
   * @param text
   *   the bill text content to embed
   * @return
   *   the embedding vector, or `None` if embedding generation failed (transient HTTP error, dimension mismatch, etc.).
   *   The pipeline persists `None`-embedding chunks so a later tick can re-process — see `BillTextProcessor`.
   */
  def generateEmbedding(text: String): F[Option[Array[Float]]]

  /**
   * Batch-embed a list of texts in a single call to the underlying model.
   *
   * Sends all inputs in one HTTP request to `/api/embed` (or equivalent), letting the model batch the forward pass
   * across them. Per benchmark on RTX 2070 SUPER + qwen3-embedding-4B (PR #71), batch=10 captures ~15% of the batching
   * benefit; for smaller models (e.g. qwen3-embedding-0.6b) the sweet spot rises to ~50.
   *
   * Error semantics mirror [[generateEmbedding]]: if the batch call fails (HTTP error, dimension mismatch, partial
   * response), the whole batch returns `List.fill(texts.size)(None)` — failures don't propagate as raised errors, they
   * degrade per-chunk. This preserves the existing partial-result contract: the pipeline persists chunk rows with
   * `embedding = None` and the next pipeline tick re-processes the bill.
   *
   * @param texts
   *   the chunks to embed, in order. Empty list returns empty list. Empty-string entries return `None` in the
   *   corresponding output position without consuming a slot in the API call.
   * @return
   *   one `Option[Array[Float]]` per input text, in the same order. `Some` for successful embeddings, `None` for
   *   per-item or batch-level failures.
   */
  def generateEmbeddings(texts: List[String]): F[List[Option[Array[Float]]]]

}
