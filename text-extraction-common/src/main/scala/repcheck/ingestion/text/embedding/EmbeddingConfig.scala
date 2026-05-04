package repcheck.ingestion.text.embedding

import scala.concurrent.duration.FiniteDuration

import pureconfig.ConfigReader

/**
 * Embedding-model configuration.
 *
 * `maxChunkChars` bounds the size of each text slice produced by the chunker before embedding. The model's effective
 * input window is the binding constraint — qwen3-embedding tops out around ~32k tokens. Empirically the sweet spot is
 * well below the limit: PLAW benchmark (PR #71 follow-up) showed 12k-char chunks complete a 4.5M-char Public Law in 9.3
 * min sequential vs 10.1 min for 20k-char chunks because attention is `O(n²)` per chunk so larger chunks pay quadratic
 * per-pass cost without proportionally fewer passes. Tunable via `OLLAMA_MAX_CHUNK_CHARS`. Must be `> 0` (validated at
 * chunker entry).
 *
 * `embedBatchSize` is the **target** number of chunks per Ollama `/api/embed` call. The cross-bill embedder accumulates
 * chunks emitted from concurrently-processing bills until `embedBatchSize` is reached, then fires the batch. With a
 * single-bill embedder this would be the upper-bound on per-call size; with the cross-bill embedder it's the value the
 * pipeline tries to hit on every call. Sweet spot on RTX 2070 SUPER + qwen3-embedding:0.6b is batch=50. Tunable via
 * `OLLAMA_EMBED_BATCH_SIZE`. Must be `> 0`.
 *
 * `embedBatchTimeout` caps how long the cross-bill embedder waits to collect a full `embedBatchSize` batch before
 * firing whatever it has. Without this, periods of low traffic (e.g., tail end of a backlog) would deadlock — lone
 * single-chunk bills would wait forever for 49 more chunks to arrive. With it, the worst-case extra latency for a small
 * bill is one timeout-window. Default 1s, tunable via `OLLAMA_EMBED_BATCH_TIMEOUT`.
 *
 * `embedQueueCapacityMultiplier` sizes the cross-bill embedder's bounded chunk queue as `embedBatchSize *
 * embedQueueCapacityMultiplier`. Headroom lets upstream extractors keep producing while the worker fiber is mid-batch —
 * too small and producers block on `queue.offer` between batches; too large and we accept more memory pressure during
 * burst traffic. Default 10× batch size is the universal sweet spot identified during the cross-bill embedder rollout.
 * Must be `> 0`. Tunable via `OLLAMA_EMBED_QUEUE_CAPACITY_MULTIPLIER`.
 */
final case class EmbeddingConfig(
  baseUrl: String,
  modelName: String,
  dimensions: Int,
  timeoutSeconds: Int,
  maxChunkChars: Int,
  embedBatchSize: Int,
  embedBatchTimeout: FiniteDuration,
  embedQueueCapacityMultiplier: Int,
) derives ConfigReader
