package repcheck.ingestion.bills.text.pipeline

import java.time.Instant
import java.util.UUID

import cats.effect.Async
import cats.syntax.all._

import fs2.Stream

import doobie._

import repcheck.ingestion.bills.common.persistence.{BillRepository, BillTextVersionRepository, TransactionRunner}
import repcheck.ingestion.bills.text.download.BillTextDownloader
import repcheck.ingestion.bills.text.embedding.{BillChunkEmbedder, BillEmbedCtx}
import repcheck.ingestion.bills.text.errors.{BillNotFoundForText, BillTextProcessingFailed}
import repcheck.ingestion.bills.text.persistence.RawBillTextRepository
import repcheck.ingestion.common.events.IngestionEventPublisher
import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}
import repcheck.ingestion.text.chunking.TextChunker
import repcheck.ingestion.text.embedding.{EmbeddingConfig, EmbeddingContextLengthExceeded, EmbeddingGenerationFailed}
import repcheck.pipeline.models.events.{BillTextAvailableEvent, BillTextIngestedEvent}
import repcheck.pipeline.models.metadata.ProcessingResult
import repcheck.shared.models.congress.common.FormatType
import repcheck.shared.models.congress.dos.bill.BillTextVersionDO

/**
 * Processes one `BillTextAvailableEvent` end-to-end. Phase 3 of the bill-text streaming refactor (see plan
 * `bill-text-10mb-streaming.md`) makes the entire pipeline streaming — extraction, chunking, embedding, and persistence
 * are now one fs2 `Stream` so heap stays bounded by the per-batch working set regardless of body size.
 *
 *   1. **Insert version row first**, with `fetched_at = NULL`. The skip-check ([[isAlreadyProcessed]]) treats
 *      `fetched_at IS NOT NULL` as the completion marker, so a row with `fetched_at = NULL` indicates "in flight or
 *      previously crashed mid-flight" and the next pipeline tick will reprocess.
 *   1. **Clear orphan chunks** for that `version_id` — DELETE any rows left by a previous failed run before
 *      re-streaming. This makes the per-chunk INSERTs idempotent against `(version_id, chunk_index)` unique constraint
 *      conflicts.
 *   1. **Open the streaming pipeline**: HTTP body bytes → streaming extractor (per-format) → streaming chunker →
 *      batched embed → per-chunk INSERT. For HTML / XML / plain-text formats the bytes flow directly from the HTTP
 *      socket into the parser — no temp file. PDF spools to a temp file inside [[PdfStreamExtractor]] (PDF requires
 *      random access to the xref table at the end of the file). Backpressure flows end-to-end: the slowest stage
 *      (embedding, ~5s per batch) gates the rate of all upstream stages.
 *   1. **Mark the version complete** via UPDATE `bill_text_versions SET fetched_at = NOW()`. After this, the next
 *      pipeline tick's skip-check will short-circuit re-processing.
 *   1. **Publish the ingested event** for downstream consumers (LLM analysis pipeline).
 *
 * ==Heap profile post-Phase-3==
 *
 * Peak heap during processing of any body size ≈ `(extractor working state, ~64 KiB) + (chunker buffer, ~12 KiB) + (one
 * batch of 50 chunks, ~600 KiB) + (one batch of 50 embeddings, ~200 KiB) ≈ 1 MiB`. A 10 MiB or 1 GiB body produce the
 * same heap footprint. The runtime of embedding (5+ s per batch on the GPU) backpressures all upstream phases via fs2's
 * pull-based model — bytes are pulled from the HTTP socket at exactly the speed embedding can consume.
 *
 * ==Crash semantics==
 *
 * Any failure between the version-row INSERT and the `markFetched` UPDATE leaves the row with `fetched_at = NULL` and
 * possibly a partial chunk list. The next pipeline tick treats it as "not yet processed", clears partial chunks, and
 * re-streams from scratch. This trades single-transaction atomicity for unbounded heap usage — acceptable because
 * embedding is by far the slowest phase and a long-held tx would hold connection locks for hours on a large bill.
 */
class BillTextProcessor[F[_]: Async] private[text] (
  downloader: BillTextDownloader[F],
  billRepository: BillRepository[ConnectionIO],
  textVersionRepository: BillTextVersionRepository[ConnectionIO],
  rawBillTextRepository: RawBillTextRepository[ConnectionIO],
  embedder: BillChunkEmbedder[F],
  embeddingConfig: EmbeddingConfig,
  eventPublisher: IngestionEventPublisher[F],
  xa: Transactor[F],
  logger: PipelineLogger[F],
  extractText: (Stream[F, Byte], String) => Stream[F, String],
) {

  private val StepName = "bill-text-processing"

  def processEvent(event: BillTextAvailableEvent, correlationId: UUID): F[ProcessingResult] = {
    val logCtx = LogContext(
      runId = correlationId.toString,
      stepName = StepName,
      correlationId = Some(correlationId),
      entityId = Some(event.naturalKey),
    )

    processEventInternal(event, correlationId, logCtx).handleErrorWith { error =>
      val errorClass = classifyError(error)
      logger.error(logCtx, s"Failed to process bill text for ${event.naturalKey}: ${error.getMessage}", Some(error)) *>
        Async[F].pure(ProcessingResult.Failed(event.naturalKey, error.getMessage, errorClass))
    }
  }

  private[pipeline] def processEventInternal(
    event: BillTextAvailableEvent,
    correlationId: UUID,
    logCtx: LogContext,
  ): F[ProcessingResult] =
    for {
      _        <- logger.info(logCtx, s"Processing bill text for ${event.naturalKey} (format=${event.textFormat})")
      dbBillId <- lookupBillId(event.naturalKey)
      alreadyProcessed <- isAlreadyProcessed(dbBillId, event.versionCode)
      result <-
        if (alreadyProcessed) {
          logger
            .info(
              logCtx,
              s"Skipping ${event.naturalKey} version=${event.versionCode} — bill_text_versions row already complete (fetched_at IS NOT NULL)",
            )
            .as(ProcessingResult.Skipped(event.naturalKey, "already-processed"))
        } else {
          processFreshBillText(event, dbBillId, correlationId, logCtx)
        }
    } yield result

  /**
   * Skip-check: returns true iff a `bill_text_versions` row exists for `(billId, versionCode)` AND its `fetched_at`
   * column is non-NULL. The non-NULL filter distinguishes complete runs (skip) from in-flight or previously-crashed
   * runs (re-process). See class scaladoc for crash semantics.
   */
  private[pipeline] def isAlreadyProcessed(billId: Long, versionCode: String): F[Boolean] =
    TransactionRunner
      .run(xa)(textVersionRepository.findByBillId(billId))
      .map(_.exists(v => v.versionCode == versionCode && v.fetchedAt.isDefined))

  private[pipeline] def processFreshBillText(
    event: BillTextAvailableEvent,
    dbBillId: Long,
    correlationId: UUID,
    logCtx: LogContext,
  ): F[ProcessingResult] = {
    val pendingVersion = buildTextVersion(event, dbBillId, fetchedAt = None)
    for {
      versionId <- persistPendingVersion(pendingVersion)
      _         <- clearOrphanChunks(versionId)
      embedderResult <- streamDownloadExtractChunkEmbedAndPersist(
        event = event,
        dbBillId = dbBillId,
        versionId = versionId,
        correlationId = correlationId,
      )
      finalResult <- embedderResult match {
        case ProcessingResult.Succeeded(_, _) =>
          for {
            _ <- markVersionFetched(versionId)
            _ <- publishEvent(event, correlationId)
            _ <- logger.info(
              logCtx,
              s"Successfully processed bill text for ${event.naturalKey} — version $versionId",
            )
          } yield ProcessingResult.Succeeded(event.naturalKey, eventEmitted = true)
        case ProcessingResult.Failed(_, reason, errorClass) =>
          // Embedder already classified the error (Transient vs Systemic) — propagate without
          // re-classifying. Re-key the entityId to the bill's natural key so downstream
          // PipelineRunSummary aggregates correctly per-bill.
          logger
            .warn(logCtx, s"Cross-bill embedder failed for ${event.naturalKey} (version $versionId): $reason")
            .as(ProcessingResult.Failed(event.naturalKey, reason, errorClass))
        case ProcessingResult.Skipped(_, reason) =>
          // The embedder is not expected to return Skipped — that would indicate a contract
          // violation (e.g. caller registered a bill but submitted no chunks). Surface as
          // a Systemic failure for operator attention.
          Async[F].raiseError[ProcessingResult](
            BillTextProcessingFailed(
              event.naturalKey,
              s"Cross-bill embedder unexpectedly returned Skipped(reason=$reason) for version $versionId",
            )
          )
      }
    } yield finalResult
  }

  private[pipeline] def lookupBillId(billNaturalKey: String): F[Long] =
    TransactionRunner.run(xa)(billRepository.findByBillId(billNaturalKey)).flatMap {
      case Some(bill) => Async[F].pure(bill.billId)
      case None       => Async[F].raiseError(BillNotFoundForText(billNaturalKey))
    }

  /**
   * Insert the parent `bill_text_versions` row in its own committed transaction. `fetched_at` is left NULL until
   * `markVersionFetched` runs at the end of the streaming flow. The companion `bills.*` columns (`text_url`, etc.) are
   * updated atomically inside the same transaction via `storeAndUpdateBill` so the bill row's pointer to the version is
   * consistent immediately.
   */
  private[pipeline] def persistPendingVersion(version: BillTextVersionDO): F[Long] =
    TransactionRunner.run(xa)(textVersionRepository.storeAndUpdateBill(version))

  /**
   * Best-effort DELETE of any leftover chunks attached to `versionId`. Idempotent: a prior successful run on this
   * version already had its chunks deleted-and-re-inserted, and a never-attempted version has no chunks at all. Any
   * orphans here come from a previous run that crashed between the version-row INSERT and `markFetched` — clearing them
   * prevents the per-chunk INSERTs from hitting the `(version_id, chunk_index)` unique constraint.
   */
  private[pipeline] def clearOrphanChunks(versionId: Long): F[Unit] =
    TransactionRunner.run(xa)(rawBillTextRepository.deleteByVersionId(versionId))

  /**
   * Build the per-bill chunk stream and submit it to the cross-bill embedder. The embedder accumulates this bill's
   * chunks alongside chunks from other concurrently-processing bills until a batch of `embedBatchSize` is ready, then
   * embeds + INSERTs the entire batch in one Ollama call + one DB transaction. The result is a `ProcessingResult` that
   * resolves when ALL of THIS bill's chunks have been INSERTed (success) or when any batch containing one of this
   * bill's chunks fails (Failed).
   *
   * Pipeline stages within this method:
   *
   *   1. `downloader.streamBody` — `Stream[F, Byte]` of HTTP response bytes for the bill's text.
   *   1. `extractText(bytes, format)` — `Stream[F, String]` of semantic fragments per the format's natural unit.
   *   1. `stripNullBytes` + `filter(nonEmpty)` — strip null bytes (Postgres TEXT can't hold them) and drop empties.
   *   1. `TextChunker.chunkPipe(maxChunkChars)` — accumulate fragments and emit fixed-size chunks.
   *   1. `embedder.processChunks` — submits each chunk to the shared cross-bill queue; awaits the bill's Deferred.
   *
   * The cross-bill batching that produces the actual Ollama call happens INSIDE the embedder's background fiber, NOT
   * inside this stream. Backpressure flows: if the embedder's queue is full, `offerChunk` blocks, the chunk stream
   * pauses, the extractor pauses, the byte stream pauses → all the way back to the HTTP socket.
   */
  private[pipeline] def streamDownloadExtractChunkEmbedAndPersist(
    event: BillTextAvailableEvent,
    dbBillId: Long,
    versionId: Long,
    correlationId: UUID,
  ): F[ProcessingResult] = {
    // No defensive check here for `maxChunkChars > 0` — `TextChunker.chunkPipe` is the single source of truth
    // for that validation. If it's misconfigured, the chunker raises `InvalidChunkSize` through the F effect channel
    // when the stream is consumed, the embedder's Resource cleanup completes the Deferred as Failed, and the
    // raise propagates back to processEvent's `handleErrorWith`.
    val ctx   = BillEmbedCtx(dbBillId = dbBillId, versionId = versionId, naturalKey = event.naturalKey)
    val bytes = downloader.streamBody(event.textUrl, event.textFormat, correlationId)
    val chunkStream = extractText(bytes, event.textFormat)
      .map(stripNullBytes)
      .filter(_.nonEmpty)
      .through(TextChunker.chunkPipe(embeddingConfig.maxChunkChars))
    embedder.processChunks(ctx, chunkStream)
  }

  /**
   * Postgres TEXT can't hold null bytes; Congress.gov occasionally serves bills with stray U+0000 chars in the rendered
   * HTML. The buffered code path scrubbed these once on the whole document; the streaming path scrubs per fragment
   * (cheaper, local).
   */
  private[pipeline] def stripNullBytes(text: String): String =
    text.filter(_.toInt != 0)

  /**
   * Mark the version row as fully-fetched by setting `fetched_at = NOW()`. After this UPDATE commits, the skip-check on
   * the next pipeline tick will short-circuit re-processing — the streaming flow is complete.
   */
  private[pipeline] def markVersionFetched(versionId: Long): F[Unit] =
    Async[F].delay(Instant.now()).flatMap { now =>
      TransactionRunner.run(xa)(textVersionRepository.markFetched(versionId, now))
    }

  private[pipeline] def buildTextVersion(
    event: BillTextAvailableEvent,
    dbBillId: Long,
    fetchedAt: Option[Instant],
  ): BillTextVersionDO = {
    val formatType = FormatType.fromString(event.textFormat).toOption
    BillTextVersionDO(
      id = 0L,
      billId = dbBillId,
      versionCode = event.versionCode,
      versionType = event.textFormat,
      versionDate = None,
      formatType = formatType,
      url = Some(event.textUrl),
      fetchedAt = fetchedAt,
      createdAt = None,
    )
  }

  private[pipeline] def publishEvent(
    event: BillTextAvailableEvent,
    correlationId: UUID,
  ): F[String] = {
    val ingestedEvent = BillTextIngestedEvent(
      naturalKey = event.naturalKey,
      versionId = correlationId,
      congress = event.congress,
      versionCode = event.versionCode,
      previousVersionCode = event.previousVersionCode,
    )
    eventPublisher.billTextIngested(ingestedEvent, correlationId)
  }

  private[pipeline] def classifyError(error: Throwable): String =
    error match {
      case _: BillNotFoundForText      => "Systemic"
      case _: BillTextProcessingFailed => "Systemic"
      // Context-length errors are deterministic for a given (chunk-size, model num_ctx) pair — retrying the same
      // oversized chunk always fails the same way. Mark Systemic so the bill is skipped instead of looping forever.
      // Operator fix is to lower OLLAMA_MAX_CHUNK_CHARS or raise the model's num_ctx in the Modelfile.
      case _: EmbeddingContextLengthExceeded  => "Systemic"
      case _: EmbeddingGenerationFailed       => "Transient"
      case _: java.net.SocketTimeoutException => "Transient"
      case _: java.net.ConnectException       => "Transient"
      case _: java.io.IOException             => "Transient"
      case _: java.sql.SQLTransientException  => "Transient"
      case _                                  => "Systemic"
    }

}
