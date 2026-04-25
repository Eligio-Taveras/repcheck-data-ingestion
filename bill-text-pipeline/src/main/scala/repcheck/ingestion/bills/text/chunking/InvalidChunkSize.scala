package repcheck.ingestion.bills.text.chunking

/**
 * The configured chunk size is non-positive. This indicates a misconfiguration of `OLLAMA_MAX_CHUNK_CHARS` (or
 * equivalent HOCON binding) and should fail the pipeline run fast rather than silently producing zero chunks for
 * non-empty bill text.
 */
final case class InvalidChunkSize(maxChunkChars: Int)
    extends RuntimeException(s"BillTextChunker requires maxChunkChars > 0, got $maxChunkChars")
