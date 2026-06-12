package repcheck.ingestion.text.embedding

import scala.concurrent.duration.DurationInt

import cats.effect.Async
import cats.effect.std.UUIDGen
import cats.syntax.all._

import org.http4s.Uri
import org.http4s.client.Client

import com.repcheck.embedding.{OllamaConfig, OllamaEmbedRequestFailed, OllamaEmbeddingClient}
import com.repcheck.utils.errors.{RetryConfig, RetryWrapper}

import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}

/**
 * [[EmbeddingService]] as a thin adapter over the shared `repcheck-embedding` client (F3b consolidation) — the wire
 * mechanics live in one place; THIS layer owns only the ingestion pipeline's error POLICY, unchanged: empty inputs
 * skip positionally, failures degrade to `None` per chunk (persisted as null-embedding rows a later tick
 * re-processes), and context-length poison inputs propagate as [[EmbeddingContextLengthExceeded]] so the pipeline
 * marks Failed-Systemic instead of retrying.
 *
 * Wire-identical to the previous in-repo implementation: same request shape (`keepAlive = None` keeps the server's
 * `OLLAMA_KEEP_ALIVE` in charge, e.g. the 24h local tuning), `maxRetries = 0` (the pipeline tick loop is the retry).
 */
class OllamaEmbeddingService[F[_]: Async: UUIDGen](
  client: Client[F],
  config: EmbeddingConfig,
  logger: PipelineLogger[F],
) extends EmbeddingService[F] {

  /** Left = invalid base URL; preserved semantics: every call logs and degrades to None rather than raising. */
  private val sharedClient: Either[String, OllamaEmbeddingClient[F]] =
    Uri
      .fromString(config.baseUrl)
      .leftMap(failure => s"Invalid Ollama URL: ${failure.message}")
      .map { baseUri =>
        new OllamaEmbeddingClient[F](
          client,
          OllamaConfig(
            baseUri = baseUri,
            model = config.modelName,
            expectedDimension = config.dimensions,
            keepAlive = None, // server policy (OLLAMA_KEEP_ALIVE) governs, exactly as before
            requestTimeout = config.timeoutSeconds.seconds,
            retry = RetryConfig(maxRetries = 0), // the pipeline tick loop is the retry, exactly as before
          ),
          new RetryWrapper[F]((_, _, _, _, _, _) => Async[F].unit),
        )
      }

  override def generateEmbedding(text: String): F[Option[Array[Float]]] =
    if (text.trim.isEmpty) {
      Async[F].pure(None)
    } else {
      generateEmbeddings(List(text)).map(_.headOption.flatten)
    }

  override def generateEmbeddings(texts: List[String]): F[List[Option[Array[Float]]]] = {
    // Index → text for non-empty inputs; empty/whitespace inputs short-circuit to None at their position so the
    // shared client (which rejects empties loudly) never sees them — the skip semantic callers rely on.
    val indexed     = texts.zipWithIndex
    val nonEmpty    = indexed.filter { case (text, _) => text.trim.nonEmpty }
    val emptyCount  = texts.size - nonEmpty.size
    val totalLength = texts.iterator.map(_.length).sum

    if (nonEmpty.isEmpty) {
      Async[F].pure(List.fill(texts.size)(None))
    } else {
      embedNonEmpty(nonEmpty.map { case (text, _) => text }).attempt.flatMap {
        case Right(embeddings) =>
          val withResult: Map[Int, Array[Float]] =
            nonEmpty.zip(embeddings).map { case ((_, originalIdx), emb) => originalIdx -> emb }.toMap
          Async[F].pure(texts.indices.toList.map(i => withResult.get(i)))
        // Context-length is NOT swallowed: retrying the same oversized input always fails the same way, so propagate
        // it so the pipeline can mark the bill Failed-Systemic. Callers classify the error.
        case Left(error: EmbeddingContextLengthExceeded) => Async[F].raiseError(error)
        case Left(error) =>
          logger
            .warn(
              LogContext(runId = "embedding", stepName = "ollama-embed-batch"),
              s"Batch embedding failed for ${nonEmpty.size.toString} chunks (${emptyCount.toString} skipped empty, total ${totalLength.toString} chars), continuing with None embeddings: ${error.getMessage}",
            )
            .as(List.fill(texts.size)(None))
      }
    }
  }

  /** Delegate to the shared client, translating its typed context-length error into this package's own. */
  private[embedding] def embedNonEmpty(texts: List[String]): F[List[Array[Float]]] =
    sharedClient match {
      case Left(invalidUrl) => Async[F].raiseError(EmbeddingGenerationFailed(invalidUrl, 0))
      case Right(shared) =>
        shared.embedBatch(texts).adaptError {
          case OllamaEmbedRequestFailed(_, poison: com.repcheck.embedding.EmbeddingContextLengthExceeded) =>
            EmbeddingContextLengthExceeded(poison.detail, poison.totalChars)
        }
    }

}
