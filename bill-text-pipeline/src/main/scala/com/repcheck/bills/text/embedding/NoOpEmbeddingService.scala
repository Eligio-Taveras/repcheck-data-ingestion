package com.repcheck.bills.text.embedding

import cats.Applicative

/**
 * No-op embedding service that always returns None.
 *
 * Used as the default until the DJL + ONNX Runtime implementation (all-MiniLM-L6-v2) is wired in a future PR. This
 * allows the pipeline to run end-to-end without requiring the ML model dependency.
 */
class NoOpEmbeddingService[F[_]: Applicative] extends EmbeddingService[F] {

  override def generateEmbedding(text: String): F[Option[Array[Float]]] =
    Applicative[F].pure(None)

}
