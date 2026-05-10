package repcheck.ingestion.amendments.text.pipeline

import java.time.Instant

import cats.effect.Async
import cats.syntax.all._

import fs2.Stream

import doobie._

import repcheck.ingestion.amendments.text.download.AmendmentTextDownloader
import repcheck.ingestion.amendments.text.embedding.{AmendmentChunkEmbedder, AmendmentEmbedCtx}
import repcheck.ingestion.amendments.text.errors.{
  AmendmentTextDownloadErrorClassifier,
  AmendmentTextDownloadHttpError,
  AmendmentTextProcessingFailed,
}
import repcheck.ingestion.amendments.text.persistence.AmendmentTextVersionRepository
import repcheck.ingestion.bills.common.persistence.TransactionRunner
import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}
import repcheck.ingestion.text.chunking.TextChunker
import repcheck.ingestion.text.embedding.{EmbeddingConfig, EmbeddingContextLengthExceeded, EmbeddingGenerationFailed}
import repcheck.pipeline.models.errors.ErrorClass
import repcheck.pipeline.models.events.AmendmentTextAvailableEvent
import repcheck.pipeline.models.metadata.ProcessingResult
import repcheck.shared.models.congress.common.FormatType
import repcheck.shared.models.congress.dos.amendment.AmendmentTextVersionDO

/**
 * Processes one [[AmendmentTextAvailableEvent]] end-to-end. Mirror of `BillTextProcessor` for the amendment side.
 *
 *   1. **Resolve the storage `FormatType`** from the wire `formatType` (`HTML` → FormattedText, `PDF` → PDF). An
 *      unknown value raises [[AmendmentTextProcessingFailed]] BEFORE the upsert — no fallback row is ever written.
 *   1. **Single-roundtrip upsert** via [[AmendmentTextVersionRepository.upsert]]. The wire `versionTypeCode`
 *      (`SUB`/`MOD`) is now exactly the value the DB enum stores (since db-migrations 0.1.36), so it is bound directly.
 *      Returns `(versionId, inserted, alreadyComplete)`. If `alreadyComplete = true`, short-circuit to
 *      `Skipped("already-ingested")` and ACK the message immediately.
 *   1. **Hand off to the embedder via [[AmendmentChunkEmbedder.submit]]** with the chunk stream + Pub/Sub ack/nack
 *      effects. The embedder owns chunk persistence, post-batch trim of the stale tail, `markFetched`, and the eventual
 *      ACK / NACK. `submit` runs synchronously on this fiber: it drives the chunk stream, force-flushes the residual at
 *      end-of-stream, and may invoke ACK / NACK before returning (or, for a sibling concurrent producer's flush that
 *      happens to bring this ackId's `submitted == expected`, the ACK fires from THAT producer's fiber mid-flush).
 *      Returns `ProcessingResult.Succeeded` once `submit` completes — the result is a stats signal for the pipeline
 *      executor; the message-level ACK has typically already fired by then.
 *
 * ==Idempotency at the chunk layer==
 *
 * `clearOrphanChunks` is gone. The chunk repository now uses last-writer-wins UPSERT on `(version_id, chunk_index)`, so
 * re-deliveries (Pub/Sub at-least-once, post-crash retry) overwrite chunk rows in place. The embedder's post-completion
 * `trimChunksPast` removes any stale tail when a re-stream produces fewer chunks than a previous run.
 *
 * ==No completion event==
 *
 * Per §7.6 spec the processor never publishes a `*.text.ingested` event. Downstream consumers (analysis, scoring)
 * discover ready amendments by polling `amendment_text_versions WHERE fetched_at IS NOT NULL`.
 */
class AmendmentTextProcessor[F[_]: Async] private[text] (
  downloader: AmendmentTextDownloader[F],
  amendmentTextVersionRepository: AmendmentTextVersionRepository[ConnectionIO],
  embedder: AmendmentChunkEmbedder[F],
  embeddingConfig: EmbeddingConfig,
  xa: Transactor[F],
  logger: PipelineLogger[F],
  extractText: (Stream[F, Byte], String, String) => Stream[F, String],
) {

  private val StepName = "amendment-text-processing"

  /**
   * Process one event. Returns:
   *   - `Skipped` when the version row is already complete (ack delegated immediately to caller).
   *   - `Succeeded` once the chunk stream has been processed by the embedder. `embedder.submit` runs synchronously on
   *     this fiber, including any flushes triggered by `batchSize` or by the residual force-flush at end-of-stream; the
   *     per-ackId ACK / NACK has typically fired by the time `submit` returns. (It can also fire from a sibling
   *     concurrent producer's `submit` if THAT producer is the one whose flush brings this ackId's `submitted ==
   *     expected`.) The `Succeeded` result here is a stats signal for the pipeline-executor's counters.
   *   - `Failed` when the version-row UPSERT raises (caller invokes `nack` directly), or when `submit` itself raises
   *     (the embedder already invoked `nack` internally before re-raising).
   */
  def processEvent(
    event: AmendmentTextAvailableEvent,
    ackId: String,
    ack: F[Unit],
    nack: F[Unit],
  ): F[ProcessingResult] = {
    val correlationId = event.correlationId
    val logCtx = LogContext(
      runId = correlationId.toString,
      stepName = StepName,
      correlationId = Some(correlationId),
      entityId = Some(event.naturalKey),
    )

    processEventInternal(event, ackId, ack, nack, logCtx).handleErrorWith { error =>
      val errorClass = classifyError(error)
      logger.error(
        logCtx,
        s"Failed to process amendment text for ${event.naturalKey}: ${error.getMessage}",
        Some(error),
      ) *> nack.attempt.void *>
        Async[F].pure(ProcessingResult.Failed(event.naturalKey, error.getMessage, errorClass))
    }
  }

  private[pipeline] def processEventInternal(
    event: AmendmentTextAvailableEvent,
    ackId: String,
    ack: F[Unit],
    nack: F[Unit],
    logCtx: LogContext,
  ): F[ProcessingResult] =
    for {
      _ <- logger.info(
        logCtx,
        s"Processing amendment text for ${event.naturalKey} (versionTypeCode=${event.versionTypeCode}, formatType=${event.formatType})",
      )
      // Resolve the format BEFORE the upsert so an unknown formatType never produces a misleading row.
      formatType <- resolveFormatType(event.naturalKey, event.formatType)
      pendingVersion = buildTextVersion(event, formatType)
      upsertResult <- upsertVersion(pendingVersion)
      result <- upsertResult match {
        case (versionId, _, true) =>
          // Already complete — fire ACK immediately; no chunks to stream.
          logger
            .info(
              logCtx,
              s"Skipping ${event.naturalKey} (versionTypeCode=${event.versionTypeCode}, format=${event.formatType}) — version $versionId already complete",
            ) *> ack
            .as(ProcessingResult.Skipped(event.naturalKey, "already-ingested"))
        case (versionId, _, false) =>
          handOffToEmbedder(event, versionId, ackId, ack, nack, logCtx)
      }
    } yield result

  /**
   * Resolve the wire `formatType` to the DB-stored [[FormatType]]. Unknown values raise
   * [[AmendmentTextProcessingFailed]] (Systemic) through the F effect channel — the natural key is in scope here so the
   * error message names the exact amendment, not `<unknown>`.
   */
  private[pipeline] def resolveFormatType(naturalKey: String, wireFormat: String): F[FormatType] =
    wireFormat match {
      case "HTML" => Async[F].pure(FormatType.FormattedText)
      case "PDF"  => Async[F].pure(FormatType.PDF)
      case other =>
        Async[F].raiseError(
          AmendmentTextProcessingFailed(naturalKey, s"Unsupported format type: '$other' (expected 'HTML' or 'PDF')")
        )
    }

  private[pipeline] def upsertVersion(version: AmendmentTextVersionDO): F[(Long, Boolean, Boolean)] =
    TransactionRunner.run(xa)(amendmentTextVersionRepository.upsert(version))

  /**
   * Hand the chunk stream off to the embedder and return immediately. The embedder owns chunk persistence + trim +
   * markFetched + ACK/NACK from this point on. Returning `Succeeded` here is a stats signal only — the actual ACK
   * lifecycle is in the embedder's hands.
   */
  private[pipeline] def handOffToEmbedder(
    event: AmendmentTextAvailableEvent,
    versionId: Long,
    ackId: String,
    ack: F[Unit],
    nack: F[Unit],
    logCtx: LogContext,
  ): F[ProcessingResult] = {
    val ctx = AmendmentEmbedCtx(
      amendmentId = event.amendmentId,
      versionId = versionId,
      naturalKey = event.naturalKey,
    )
    val bytes = downloader.streamBody(event.url, event.formatType, event.correlationId)
    val chunkStream = extractText(bytes, event.formatType, event.naturalKey)
      .map(stripNullBytes)
      .filter(_.nonEmpty)
      .through(TextChunker.chunkPipe(embeddingConfig.maxChunkChars))
    embedder.submit(ctx, chunkStream, ackId, ack, nack) *>
      logger
        .info(logCtx, s"Submitted ${event.naturalKey} chunks to embedder; version $versionId")
        .as(ProcessingResult.Succeeded(event.naturalKey, eventEmitted = false))
  }

  /** Postgres TEXT can't hold null bytes; defensive per-fragment scrub. */
  private[pipeline] def stripNullBytes(text: String): String =
    text.filter(_.toInt != 0)

  /**
   * Build the `AmendmentTextVersionDO` that goes into the upsert. The `versionType` is the wire `versionTypeCode`
   * verbatim — both `SUB` and `MOD` are valid values of the `amendment_text_version_code_type` enum as of db-migrations
   * 0.1.36, so no translation is needed.
   */
  private[pipeline] def buildTextVersion(
    event: AmendmentTextAvailableEvent,
    formatType: FormatType,
  ): AmendmentTextVersionDO =
    AmendmentTextVersionDO(
      id = 0L,
      amendmentId = event.amendmentId,
      versionType = event.versionTypeCode,
      versionDate = event.publishedDate.getOrElse(Instant.EPOCH),
      formatType = formatType,
      url = event.url,
      // Populated only when the rewriter recognized the URL — `None` otherwise so the column reflects whether
      // the api.govinfo.gov path was used.
      downloadUrl = downloader.previewDownloadUrl(event.url),
      textLength = None,
      fetchedAt = None,
      createdAt = None,
    )

  private[pipeline] def classifyError(error: Throwable): String =
    error match {
      case http: AmendmentTextDownloadHttpError =>
        AmendmentTextDownloadErrorClassifier.classify(http) match {
          case ErrorClass.Transient => "Transient"
          case ErrorClass.Systemic  => "Systemic"
        }
      case _: AmendmentTextProcessingFailed        => "Systemic"
      case _: EmbeddingContextLengthExceeded       => "Systemic"
      case _: EmbeddingGenerationFailed            => "Transient"
      case _: java.net.SocketTimeoutException      => "Transient"
      case _: java.net.ConnectException            => "Transient"
      case _: java.io.IOException                  => "Transient"
      case _: java.sql.SQLTransientException       => "Transient"
      case _: org.http4s.ember.core.EmberException => "Transient"
      case _                                       => "Systemic"
    }

}
