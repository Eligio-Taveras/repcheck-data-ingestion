package repcheck.ingestion.bills.text.embedding

final case class EmbeddingGenerationFailed(
  reason: String,
  textLength: Int,
) extends Exception(s"Embedding generation failed for text of length $textLength: $reason")
