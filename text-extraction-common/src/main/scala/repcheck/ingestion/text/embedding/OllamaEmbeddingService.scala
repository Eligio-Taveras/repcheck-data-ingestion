package repcheck.ingestion.text.embedding

import cats.effect.Async
import cats.syntax.all._

import io.circe.{Decoder, Encoder, Json}

import org.http4s.circe.{jsonEncoderOf, jsonOf}
import org.http4s.client.Client
import org.http4s.{EntityDecoder, EntityEncoder, Method, Request, Response, Uri}

import repcheck.ingestion.common.logging.PipelineLogger

/**
 * Embedding service backed by an Ollama instance.
 *
 * Calls the Ollama `/api/embed` endpoint to generate embeddings using a configurable model and dimension. The Ollama
 * instance can be local (dev) or a Cloud Run sidecar (prod).
 *
 * Both `generateEmbedding` (single) and `generateEmbeddings` (batch) hit the same `/api/embed` endpoint — Ollama's
 * `input` field accepts either a JSON string or a JSON array of strings. The batch path lets the model amortize
 * tokenization, kernel-launch overhead, and HTTP round-trip cost across multiple chunks; sweet spot on RTX 2070 SUPER +
 * qwen3-embedding:0.6b (current default) is batch=50 — the smaller model frees ~5 GB of VRAM vs the 4B baseline,
 * lifting GPU-saturation batch size from ~10 to ~50.
 */
class OllamaEmbeddingService[F[_]: Async](
  client: Client[F],
  config: EmbeddingConfig,
  logger: PipelineLogger[F],
) extends EmbeddingService[F] {

  /**
   * Request body for `/api/embed`. `input` is encoded as a JSON array — Ollama accepts both single-string and array
   * forms, but encoding a length-1 array works for both shapes and lets a single code path handle both single and
   * batched calls.
   */
  private case class EmbedRequest(
    model: String,
    input: List[String],
    dimensions: Int,
  )

  private object EmbedRequest {

    implicit val encoder: Encoder[EmbedRequest] = Encoder.instance { req =>
      Json.obj(
        "model"      -> Json.fromString(req.model),
        "input"      -> Json.arr(req.input.map(Json.fromString)*),
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
      generateEmbeddings(List(text)).map(_.headOption.flatten)
    }

  override def generateEmbeddings(texts: List[String]): F[List[Option[Array[Float]]]] = {
    // Index → text for non-empty inputs; empty/whitespace inputs short-circuit to None at their position so we never
    // send them to Ollama (Ollama errors on empty `input` strings, and a None at that index is the correct semantic).
    val indexed     = texts.zipWithIndex
    val nonEmpty    = indexed.filter { case (text, _) => text.trim.nonEmpty }
    val emptyCount  = texts.size - nonEmpty.size
    val totalLength = texts.iterator.map(_.length).sum

    if (nonEmpty.isEmpty) {
      Async[F].pure(List.fill(texts.size)(None))
    } else {
      callOllama(nonEmpty.map { case (text, _) => text }).attempt.flatMap {
        case Right(embeddings) =>
          // Reconstruct the output list with None at empty-input positions and Some(embedding) at the rest.
          val withResult: Map[Int, Array[Float]] =
            nonEmpty.zip(embeddings).map { case ((_, originalIdx), emb) => originalIdx -> emb }.toMap
          Async[F].pure(texts.indices.toList.map(i => withResult.get(i)))
        // EmbeddingContextLengthExceeded is NOT swallowed: retrying the same oversized input always fails the same
        // way, so propagate it so the pipeline can mark the bill Failed-Systemic. Callers classify the error.
        case Left(error: EmbeddingContextLengthExceeded) => Async[F].raiseError(error)
        case Left(error) =>
          logger
            .warn(
              repcheck.ingestion.common.logging.LogContext(
                runId = "embedding",
                stepName = "ollama-embed-batch",
              ),
              s"Batch embedding failed for ${nonEmpty.size.toString} chunks (${emptyCount.toString} skipped empty, total ${totalLength.toString} chars), continuing with None embeddings: ${error.getMessage}",
            )
            .as(List.fill(texts.size)(None))
      }
    }
  }

  private[embedding] def callOllama(texts: List[String]): F[List[Array[Float]]] =
    for {
      uri <- parseUri(s"${config.baseUrl}/api/embed")
      request = Request[F](method = Method.POST, uri = uri)
        .withEntity(EmbedRequest(config.modelName, texts, config.dimensions))
      response   <- client.expectOr[EmbedResponse](request)(classifyOllamaError(texts))
      embeddings <- extractEmbeddings(response, texts)
      _          <- embeddings.traverse_(emb => validateDimension(emb, totalCharsOf(texts)))
    } yield embeddings

  private def totalCharsOf(texts: List[String]): Int = texts.iterator.map(_.length).sum

  /**
   * Classify a non-2xx Ollama response into a typed exception. HTTP 400 with a body containing "context length" is
   * routed to [[EmbeddingContextLengthExceeded]] so the pipeline can mark Failed-Systemic and skip retry; everything
   * else is the generic [[EmbeddingGenerationFailed]] (caught and degraded to None at the batch level).
   */
  private def classifyOllamaError(texts: List[String])(response: Response[F]): F[Throwable] =
    response.bodyText.compile.string.map { body =>
      val totalChars     = totalCharsOf(texts)
      val isContextError = response.status.code === 400 && body.toLowerCase.contains("context length")
      if (isContextError) {
        EmbeddingContextLengthExceeded(body.trim, totalChars)
      } else {
        EmbeddingGenerationFailed(s"HTTP ${response.status.code.toString}: ${body.trim}", totalChars)
      }
    }

  private def parseUri(raw: String): F[Uri] =
    Async[F].fromEither(
      Uri.fromString(raw).leftMap(failure => EmbeddingGenerationFailed(s"Invalid Ollama URL: ${failure.message}", 0))
    )

  /**
   * Convert Ollama's nested `embeddings: List[List[Float]]` response into a `List[Array[Float]]` with the same length
   * and order as the input texts. Raises [[EmbeddingGenerationFailed]] if the response is empty or has a different
   * cardinality than the request — both are protocol violations that callers must handle (and `generateEmbeddings`
   * does, by swallowing into `List.fill(N)(None)`).
   */
  private def extractEmbeddings(response: EmbedResponse, texts: List[String]): F[List[Array[Float]]] = {
    val totalChars = totalCharsOf(texts)
    if (response.embeddings.isEmpty) {
      Async[F].raiseError(EmbeddingGenerationFailed("Ollama returned empty embeddings array", totalChars))
    } else if (response.embeddings.size =!= texts.size) {
      Async[F].raiseError(
        EmbeddingGenerationFailed(
          s"Embedding count mismatch: requested ${texts.size.toString} but got ${response.embeddings.size.toString}",
          totalChars,
        )
      )
    } else {
      Async[F].pure(response.embeddings.map(_.toArray))
    }
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
