package repcheck.ingestion.bills.text.embedding

import pureconfig.ConfigReader

/**
 * Embedding-model configuration.
 *
 * `maxChunkChars` bounds the size of each text slice produced by `BillTextChunker` before embedding. The model's
 * effective input window is the binding constraint — qwen3-embedding tops out around ~32k tokens. Empirically the sweet
 * spot is well below the limit: PLAW benchmark (PR #71 follow-up) showed 12k-char chunks complete a 4.5M-char Public
 * Law in 9.3 min sequential vs 10.1 min for 20k-char chunks because attention is `O(n²)` per chunk so larger chunks pay
 * quadratic per-pass cost without proportionally fewer passes. Tunable via `OLLAMA_MAX_CHUNK_CHARS`. Must be `> 0`
 * (validated at chunker entry).
 *
 * `embedBatchSize` controls how many chunks are sent in a single `/api/embed` array call by
 * [[OllamaEmbeddingService.generateEmbeddings]]. Per benchmark on RTX 2070 SUPER + qwen3-embedding-4B, batch=10
 * captures ~15% of the available batching benefit (HTTP/JSON amortization + slightly better GPU tensor-core
 * utilization); going higher hits diminishing returns quickly because the GPU is already saturated by single calls for
 * the 4B model. For smaller models (e.g. qwen3-embedding-0.6b) the GPU has more headroom and the sweet spot rises to
 * ~50. Tunable via `OLLAMA_EMBED_BATCH_SIZE`. Must be `> 0`.
 */
final case class EmbeddingConfig(
  baseUrl: String,
  modelName: String,
  dimensions: Int,
  timeoutSeconds: Int,
  maxChunkChars: Int,
  embedBatchSize: Int,
) derives ConfigReader
