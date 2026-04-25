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
 * [[OllamaEmbeddingService.generateEmbeddings]]. Sweet spot on RTX 2070 SUPER + qwen3-embedding:0.6b (current default)
 * is batch=50 — the smaller model frees ~5 GB of VRAM vs the 4B baseline, lifting GPU-saturation batch size from ~10 to
 * ~50. Override lower if running against a larger model (the 4B / 8B variants saturate at ~10 with the GPU already
 * pinned by single calls). Tunable via `OLLAMA_EMBED_BATCH_SIZE`. Must be `> 0`.
 */
final case class EmbeddingConfig(
  baseUrl: String,
  modelName: String,
  dimensions: Int,
  timeoutSeconds: Int,
  maxChunkChars: Int,
  embedBatchSize: Int,
) derives ConfigReader
