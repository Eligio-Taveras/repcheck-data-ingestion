package repcheck.ingestion.text.embedding

import cats.Applicative

/**
 * No-op embedding service that always returns None.
 *
 * Used as the default until the DJL + ONNX Runtime implementation (all-MiniLM-L6-v2) is wired in a future PR. Allows
 * pipelines to run end-to-end without requiring the ML model dependency.
 */
class NoOpEmbeddingService[F[_]: Applicative] extends EmbeddingService[F] {

  override def generateEmbedding(text: String): F[Option[Array[Float]]] =
    Applicative[F].pure(None)

  override def generateEmbeddings(texts: List[String]): F[List[Option[Array[Float]]]] =
    Applicative[F].pure(List.fill(texts.size)(None))

}
