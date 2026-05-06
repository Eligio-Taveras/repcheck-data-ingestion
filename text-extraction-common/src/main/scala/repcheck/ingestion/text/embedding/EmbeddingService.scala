package repcheck.ingestion.text.embedding

trait EmbeddingService[F[_]] {

  /**
   * Generate a vector embedding for a single text.
   *
   * Convenience wrapper over [[generateEmbeddings]] for the single-input case. Implementations should produce a
   * fixed-dimensional embedding vector suitable for semantic similarity search; the underlying call shape is whatever
   * the backing model expects (Ollama's `/api/embed` endpoint accepts both single and array forms).
   *
   * @param text
   *   the text content to embed
   * @return
   *   the embedding vector, or `None` if embedding generation failed (transient HTTP error, dimension mismatch, etc.).
   *   Callers typically persist `None`-embedding rows so a later tick can re-process.
   */
  def generateEmbedding(text: String): F[Option[Array[Float]]]

  /**
   * Batch-embed a list of texts in a single call to the underlying model.
   *
   * Sends all inputs in one HTTP request to `/api/embed` (or equivalent), letting the model batch the forward pass
   * across them. Sweet spot on RTX 2070 SUPER + qwen3-embedding:0.6b (current default) is batch=50 — the smaller model
   * frees ~5 GB of VRAM vs the 4B baseline, lifting GPU-saturation batch size from ~10 to ~50. Override lower if
   * running against a larger model (the 4B / 8B variants saturate at ~10 with the GPU already pinned).
   *
   * Error semantics mirror [[generateEmbedding]]: if the batch call fails (HTTP error, dimension mismatch, partial
   * response), the whole batch returns `List.fill(texts.size)(None)` — failures don't propagate as raised errors, they
   * degrade per-chunk. This preserves the partial-result contract: the caller persists rows with `embedding = None` and
   * the next pipeline tick re-processes.
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
