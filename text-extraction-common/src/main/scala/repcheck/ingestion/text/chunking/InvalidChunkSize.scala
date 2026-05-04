package repcheck.ingestion.text.chunking

/**
 * The configured chunk size is non-positive. This indicates a misconfiguration of the chunker's max-chars setting and
 * should fail the pipeline run fast rather than silently producing zero chunks for non-empty input.
 */
final case class InvalidChunkSize(maxChunkChars: Int)
    extends RuntimeException(s"TextChunker requires maxChunkChars > 0, got $maxChunkChars")
