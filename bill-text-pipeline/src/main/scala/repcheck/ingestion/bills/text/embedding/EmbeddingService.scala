package repcheck.ingestion.bills.text.embedding

trait EmbeddingService[F[_]] {

  /**
   * Generate a vector embedding for the given text content.
   *
   * Implementations should use a sentence-transformer model (e.g., all-MiniLM-L6-v2) to produce a fixed-dimensional
   * embedding vector suitable for semantic similarity search.
   *
   * @param text
   *   the bill text content to embed
   * @return
   *   the embedding vector, or None if embedding generation is not available
   */
  def generateEmbedding(text: String): F[Option[Array[Float]]]

}
