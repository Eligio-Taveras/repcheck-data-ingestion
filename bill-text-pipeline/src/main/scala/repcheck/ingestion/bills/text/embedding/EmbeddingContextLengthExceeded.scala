package repcheck.ingestion.bills.text.embedding

/**
 * Raised when the embedding model rejects an input because it exceeds the model's context window.
 *
 * Distinct from [[EmbeddingGenerationFailed]] (the generic transient/recoverable family) because retrying the same
 * oversized input always fails the same way — the pipeline must classify this as Systemic and skip the bill so a
 * pathological chunk size doesn't cause an infinite retry loop.
 *
 * Surfaces in [[OllamaEmbeddingService]] when Ollama returns HTTP 400 with a response body containing the substring
 * "context length" (matches "input length exceeds context length" and similar variants the runner emits — see
 * https://github.com/ollama/ollama runner.go for the canonical message). Triggered when `num_ctx` on the loaded model
 * is smaller than the tokenized input length.
 *
 * @param reason
 *   the raw response body from the embedding service, useful for diagnosing whether the cause is a too-small `num_ctx`
 *   (Modelfile-baked) or a too-large `OLLAMA_MAX_CHUNK_CHARS` config drift.
 * @param textLength
 *   the total character count of the input texts (sum across the batch). Lets ops correlate which chunk-size profile
 *   triggered the violation.
 */
final case class EmbeddingContextLengthExceeded(
  reason: String,
  textLength: Int,
) extends Exception(
      s"Embedding context length exceeded for input of total length $textLength: $reason"
    )
