package repcheck.ingestion.bills.text.embedding

import cats.effect.Async
import cats.syntax.all._

import io.circe.{Decoder, Encoder, Json}

import org.http4s.circe.{jsonEncoderOf, jsonOf}
import org.http4s.client.Client
import org.http4s.{EntityDecoder, EntityEncoder, Method, Request, Uri}

import repcheck.ingestion.common.logging.PipelineLogger

/**
 * Embedding service backed by an Ollama instance.
 *
 * Calls the Ollama `/api/embed` endpoint to generate embeddings using a configurable model and dimension. The Ollama
 * instance can be local (dev) or a Cloud Run sidecar (prod).
 */
class OllamaEmbeddingService[F[_]: Async] private[text] (
  client: Client[F],
  config: EmbeddingConfig,
  logger: PipelineLogger[F],
) extends EmbeddingService[F] {

  private case class EmbedRequest(
    model: String,
    input: String,
    dimensions: Int,
  )

  private object EmbedRequest {

    implicit val encoder: Encoder[EmbedRequest] = Encoder.instance { req =>
      Json.obj(
        "model"      -> Json.fromString(req.model),
        "input"      -> Json.fromString(req.input),
        "dimensions" -> Json.fromInt(req.dimensions),
      )
    }

  }

  private case class EmbedResponse(
    embeddings: List[List[Float]]
  )

  private object EmbedResponse {

    implicit val decoder: Decoder[EmbedResponse] =
      Decoder.forProduct1("embeddings")(EmbedResponse.apply)

  }

  implicit private val requestEncoder: EntityEncoder[F, EmbedRequest] =
    jsonEncoderOf[F, EmbedRequest]

  implicit private val responseDecoder: EntityDecoder[F, EmbedResponse] =
    jsonOf[F, EmbedResponse]

  override def generateEmbedding(text: String): F[Option[Array[Float]]] =
    if (text.trim.isEmpty) {
      Async[F].pure(None)
    } else {
      callOllama(text).map(Some(_)).handleErrorWith { error =>
        logger
          .warn(
            repcheck.ingestion.common.logging.LogContext(
              runId = "embedding",
              stepName = "ollama-embed",
            ),
            s"Embedding generation failed, continuing without embedding: ${error.getMessage}",
          )
          .as(None)
      }
    }

  private[embedding] def callOllama(text: String): F[Array[Float]] =
    for {
      uri <- parseUri(s"${config.baseUrl}/api/embed")
      request = Request[F](method = Method.POST, uri = uri)
        .withEntity(EmbedRequest(config.modelName, text, config.dimensions))
      response  <- client.expect[EmbedResponse](request)
      embedding <- extractEmbedding(response, text.length)
      _         <- validateDimension(embedding, text.length)
    } yield embedding

  private def parseUri(raw: String): F[Uri] =
    Async[F].fromEither(
      Uri.fromString(raw).leftMap(failure => EmbeddingGenerationFailed(s"Invalid Ollama URL: ${failure.message}", 0))
    )

  private def extractEmbedding(response: EmbedResponse, textLength: Int): F[Array[Float]] =
    response.embeddings match {
      case head :: _ => Async[F].pure(head.toArray)
      case Nil =>
        Async[F].raiseError(
          EmbeddingGenerationFailed("Ollama returned empty embeddings array", textLength)
        )
    }

  private def validateDimension(embedding: Array[Float], textLength: Int): F[Unit] =
    if (embedding.length =!= config.dimensions) {
      Async[F].raiseError(
        EmbeddingGenerationFailed(
          s"Dimension mismatch: expected ${config.dimensions.toString} but got ${embedding.length.toString}",
          textLength,
        )
      )
    } else {
      Async[F].unit
    }

}
