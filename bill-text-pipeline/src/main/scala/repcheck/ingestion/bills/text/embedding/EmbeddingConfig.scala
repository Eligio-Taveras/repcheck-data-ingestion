package repcheck.ingestion.bills.text.embedding

import pureconfig.ConfigReader

/**
 * Embedding-model configuration.
 *
 * `maxChunkChars` bounds the size of each text slice produced by `BillTextChunker` before embedding. The model's
 * effective input window is the binding constraint — qwen3-embedding tops out around ~32k tokens but real bill text is
 * heavily ASCII so a conservative character-window of ~30k stays well inside the limit while keeping chunk count
 * manageable. Bills bigger than the window (the 14 MB Statutes-at-Large case that motivated migration 026) split into
 * many chunks, each independently embedded and stored as a `raw_bill_text` row.
 *
 * Tunable via `OLLAMA_MAX_CHUNK_CHARS`. Must be `> 0` (validated at chunker entry).
 */
final case class EmbeddingConfig(
  baseUrl: String,
  modelName: String,
  dimensions: Int,
  timeoutSeconds: Int,
  maxChunkChars: Int,
) derives ConfigReader
