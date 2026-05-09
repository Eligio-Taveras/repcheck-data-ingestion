package repcheck.ingestion.amendments.text.pipeline

import java.time.Instant

import cats.effect.Async
import cats.syntax.all._

import fs2.Stream

import doobie._

import repcheck.ingestion.amendments.text.download.AmendmentTextDownloader
import repcheck.ingestion.amendments.text.embedding.{AmendmentChunkEmbedder, AmendmentEmbedCtx}
import repcheck.ingestion.amendments.text.errors.{AmendmentTextProcessingFailed, UnsupportedAmendmentTextVersionCode}
import repcheck.ingestion.amendments.text.persistence.{
  AmendmentTextChunkRepository,
  AmendmentTextVersionRepository,
  AmendmentTextVersionTypeMapping,
}
import repcheck.ingestion.bills.common.persistence.TransactionRunner
import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}
import repcheck.ingestion.text.chunking.TextChunker
import repcheck.ingestion.text.embedding.{EmbeddingConfig, EmbeddingContextLengthExceeded, EmbeddingGenerationFailed}
import repcheck.pipeline.models.events.AmendmentTextAvailableEvent
import repcheck.pipeline.models.metadata.ProcessingResult
import repcheck.shared.models.congress.common.FormatType
import repcheck.shared.models.congress.dos.amendment.AmendmentTextVersionDO

/**
 * Processes one [[AmendmentTextAvailableEvent]] end-to-end. Mirror of
 * [[repcheck.ingestion.bills.text.pipeline.BillTextProcessor]] for the amendment side. Crash semantics, heap profile,
 * and back-pressure behavior are identical to the bill-side processor.
 *
 *   1. **Translate the wire `versionTypeCode`** (`SUB`/`MOD`) to the stored enum value (`Submitted`/`Modified`) via
 *      [[AmendmentTextVersionTypeMapping.wireToStored]]. Unrecognized → Systemic failure.
 *   1. **Single-roundtrip upsert** via [[AmendmentTextVersionRepository.upsert]]. Returns `(versionId, inserted,
 *      alreadyComplete)`. If `alreadyComplete = true` (existing complete row, no newer `versionDate` upstream),
 *      short-circuit to `Skipped("already-ingested")` and ACK the message.
 *   1. **Clear orphan chunks** for this `version_id` (idempotent — no-op when row was just inserted).
 *   1. **Open the streaming pipeline**: HTTP body bytes → format-dispatched extractor (HTML/PDF) → chunker →
 *      cross-amendment embedder → per-batch INSERT. Backpressure flows end-to-end as in the bill side.
 *   1. **Mark the version complete** via UPDATE `amendment_text_versions SET fetched_at = NOW(), text_length = $bytes`.
 *      **No completion event is emitted** — readiness is signaled by `fetched_at IS NOT NULL`.
 *
 * ==No completion event==
 *
 * Per §7.6 spec the processor never publishes a `*.text.ingested` event. Downstream consumers (analysis, scoring)
 * discover ready amendments by polling `amendment_text_versions WHERE fetched_at IS NOT NULL`. The partial index
 * `idx_amendment_text_versions_fetched_not_null` from db-migration 039 makes that polling cheap.
 */
class AmendmentTextProcessor[F[_]: Async] private[text] (
  downloader: AmendmentTextDownloader[F],
  amendmentTextVersionRepository: AmendmentTextVersionRepository[ConnectionIO],
  amendmentTextChunkRepository: AmendmentTextChunkRepository[ConnectionIO],
  embedder: AmendmentChunkEmbedder[F],
  embeddingConfig: EmbeddingConfig,
  xa: Transactor[F],
  logger: PipelineLogger[F],
  extractText: (Stream[F, Byte], String) => Stream[F, String],
) {

  private val StepName = "amendment-text-processing"

  def processEvent(event: AmendmentTextAvailableEvent): F[ProcessingResult] = {
    val correlationId = event.correlationId
    val logCtx = LogContext(
      runId = correlationId.toString,
      stepName = StepName,
      correlationId = Some(correlationId),
      entityId = Some(event.naturalKey),
    )

    processEventInternal(event, logCtx).handleErrorWith { error =>
      val errorClass = classifyError(error)
      logger.error(
        logCtx,
        s"Failed to process amendment text for ${event.naturalKey}: ${error.getMessage}",
        Some(error),
      ) *>
        Async[F].pure(ProcessingResult.Failed(event.naturalKey, error.getMessage, errorClass))
    }
  }

  private[pipeline] def processEventInternal(
    event: AmendmentTextAvailableEvent,
    logCtx: LogContext,
  ): F[ProcessingResult] =
    for {
      _ <- logger.info(
        logCtx,
        s"Processing amendment text for ${event.naturalKey} (versionTypeCode=${event.versionTypeCode}, formatType=${event.formatType})",
      )
      storedVersion <- mapWireVersionType(event.versionTypeCode)
      pendingVersion = buildTextVersion(event, storedVersion)
      upsertResult <- upsertVersion(pendingVersion)
      result <- upsertResult match {
        case (versionId, _, true) =>
          logger
            .info(
              logCtx,
              s"Skipping ${event.naturalKey} (versionTypeCode=${event.versionTypeCode}, format=${event.formatType}) — version $versionId already complete",
            )
            .as(ProcessingResult.Skipped(event.naturalKey, "already-ingested"))
        case (versionId, _, false) =>
          processFreshVersion(event, versionId, logCtx)
      }
    } yield result

  /**
   * Map the wire-format `versionTypeCode` ("SUB"/"MOD") to the stored enum value ("Submitted"/"Modified") via
   * [[AmendmentTextVersionTypeMapping]]. An unrecognized code raises [[UnsupportedAmendmentTextVersionCode]] through
   * the F effect channel — the outer `handleErrorWith` classifies that as Systemic.
   */
  private[pipeline] def mapWireVersionType(wire: String): F[String] =
    AmendmentTextVersionTypeMapping.wireToStored(wire) match {
      case Right(stored) => Async[F].pure(stored)
      case Left(error)   => Async[F].raiseError(error)
    }

  private[pipeline] def upsertVersion(version: AmendmentTextVersionDO): F[(Long, Boolean, Boolean)] =
    TransactionRunner.run(xa)(amendmentTextVersionRepository.upsert(version))

  private[pipeline] def processFreshVersion(
    event: AmendmentTextAvailableEvent,
    versionId: Long,
    logCtx: LogContext,
  ): F[ProcessingResult] =
    for {
      _ <- clearOrphanChunks(versionId)
      embedderResult <- streamDownloadExtractChunkEmbedAndPersist(
        event = event,
        versionId = versionId,
      )
      finalResult <- embedderResult match {
        case ProcessingResult.Succeeded(_, _) =>
          for {
            _ <- markVersionFetched(versionId)
            _ <- logger.info(
              logCtx,
              s"Successfully processed amendment text for ${event.naturalKey} — version $versionId",
            )
          } yield ProcessingResult.Succeeded(event.naturalKey, eventEmitted = false)
        case ProcessingResult.Failed(_, reason, errorClass) =>
          logger
            .warn(logCtx, s"Cross-amendment embedder failed for ${event.naturalKey} (version $versionId): $reason")
            .as(ProcessingResult.Failed(event.naturalKey, reason, errorClass))
        case ProcessingResult.Skipped(_, reason) =>
          Async[F].raiseError[ProcessingResult](
            AmendmentTextProcessingFailed(
              event.naturalKey,
              s"Cross-amendment embedder unexpectedly returned Skipped(reason=$reason) for version $versionId",
            )
          )
      }
    } yield finalResult

  private[pipeline] def clearOrphanChunks(versionId: Long): F[Unit] =
    TransactionRunner.run(xa)(amendmentTextChunkRepository.deleteByVersionId(versionId))

  /**
   * Build the per-amendment chunk stream and submit it to the cross-amendment embedder. Pipeline stages within this
   * method:
   *
   *   1. `downloader.streamBody` — `Stream[F, Byte]` of HTTP response bytes. URL is rewritten to api.govinfo.gov when
   *      it matches the CREC pattern.
   *   1. `extractText(bytes, formatType)` — `Stream[F, String]` of semantic fragments (paragraphs / page text).
   *   1. `stripNullBytes` + `filter(nonEmpty)` — Postgres TEXT can't hold null bytes; defensive scrub.
   *   1. `TextChunker.chunkPipe(maxChunkChars)` — accumulate fragments + emit fixed-size chunks.
   *   1. `embedder.processChunks` — submits each chunk to the shared cross-amendment queue; awaits the amendment's
   *      Deferred.
   */
  private[pipeline] def streamDownloadExtractChunkEmbedAndPersist(
    event: AmendmentTextAvailableEvent,
    versionId: Long,
  ): F[ProcessingResult] = {
    val ctx   = AmendmentEmbedCtx(amendmentId = event.amendmentId, versionId = versionId, naturalKey = event.naturalKey)
    val bytes = downloader.streamBody(event.url, event.formatType, event.correlationId)
    val chunkStream = extractText(bytes, event.formatType)
      .map(stripNullBytes)
      .filter(_.nonEmpty)
      .through(TextChunker.chunkPipe(embeddingConfig.maxChunkChars))
    embedder.processChunks(ctx, chunkStream)
  }

  /** Postgres TEXT can't hold null bytes; defensive per-fragment scrub. */
  private[pipeline] def stripNullBytes(text: String): String =
    text.filter(_.toInt != 0)

  /**
   * Mark the version row complete: set `fetched_at = NOW()` and `text_length` to the actual character total of the
   * persisted chunks (`SUM(LENGTH(content))` from the chunks table). Computing it post-write avoids threading a counter
   * Ref through the streaming pipeline; the cost is one SUM aggregation per amendment which is cheap for the chunk
   * volume per amendment. After this UPDATE commits, downstream consumers polling `fetched_at IS NOT NULL` see the row.
   *
   * `text_length` is bounded to `Int.MaxValue` defensively — the column is `INT` per migration 039 and a pathological
   * multi-GB document would overflow. In practice amendment text fits comfortably within int range.
   */
  private[pipeline] def markVersionFetched(versionId: Long): F[Unit] =
    Async[F].delay(Instant.now()).flatMap { now =>
      TransactionRunner.run(xa) {
        for {
          totalChars <- amendmentTextChunkRepository.sumContentLengthByVersionId(versionId)
          clamped = math.min(totalChars, Int.MaxValue.toLong).toInt
          _ <- amendmentTextVersionRepository.markFetched(versionId, now, clamped)
        } yield ()
      }
    }

  private[pipeline] def buildTextVersion(
    event: AmendmentTextAvailableEvent,
    storedVersionType: String,
  ): AmendmentTextVersionDO = {
    val formatType = event.formatType match {
      case "HTML" => FormatType.FormattedText
      case "PDF"  => FormatType.PDF
      case _      => FormatType.FormattedText // fallback; extractor will raise on unknowns
    }
    AmendmentTextVersionDO(
      id = 0L,
      amendmentId = event.amendmentId,
      versionType = storedVersionType,
      versionDate = event.publishedDate.getOrElse(Instant.EPOCH),
      formatType = formatType,
      url = event.url,
      // Populated only when the rewriter recognized the URL — `None` otherwise so the column reflects whether
      // the api.govinfo.gov path was used. The `rewriter_miss` outcome counter (per §7.6 observability) reads
      // off the `download_url IS NULL AND fetched_at IS NOT NULL` predicate.
      downloadUrl = downloader.previewDownloadUrl(event.url),
      textLength = None,
      fetchedAt = None,
      createdAt = None,
    )
  }

  private[pipeline] def classifyError(error: Throwable): String =
    error match {
      case _: UnsupportedAmendmentTextVersionCode  => "Systemic"
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
