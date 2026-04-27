package repcheck.ingestion.bills.text.pipeline

import java.nio.file.Path
import java.time.Instant
import java.util.UUID

import cats.effect.Async
import cats.syntax.all._

import doobie._

import repcheck.ingestion.bills.common.persistence.{BillRepository, BillTextVersionRepository, TransactionRunner}
import repcheck.ingestion.bills.text.chunking.{BillTextChunker, InvalidChunkSize}
import repcheck.ingestion.bills.text.download.BillTextDownloader
import repcheck.ingestion.bills.text.embedding.{
  EmbeddingConfig,
  EmbeddingContextLengthExceeded,
  EmbeddingGenerationFailed,
  EmbeddingService,
}
import repcheck.ingestion.bills.text.errors.{BillNotFoundForText, BillTextProcessingFailed}
import repcheck.ingestion.bills.text.persistence.RawBillTextRepository
import repcheck.ingestion.common.events.IngestionEventPublisher
import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}
import repcheck.pipeline.models.events.{BillTextAvailableEvent, BillTextIngestedEvent}
import repcheck.pipeline.models.metadata.ProcessingResult
import repcheck.shared.models.congress.common.FormatType
import repcheck.shared.models.congress.dos.bill.{BillTextVersionDO, RawBillTextDO}

/**
 * Processes one `BillTextAvailableEvent` end-to-end. Phase 2 of the bill-text streaming refactor (see plan
 * `bill-text-10mb-streaming.md`) replaces the pre-existing buffered-`String` flow with a streaming-to-temp-file flow:
 *
 *   1. **Insert version row first**, with `fetched_at = NULL`. The skip-check ([[isAlreadyProcessed]]) treats
 *      `fetched_at IS NOT NULL` as the completion marker, so a row with `fetched_at = NULL` indicates "in flight or
 *      previously crashed mid-flight" and the next pipeline tick will reprocess.
 *   1. **Clear orphan chunks** for that `version_id` — DELETE any rows left by a previous failed run before
 *      re-streaming. This makes the per-chunk INSERTs idempotent against `(version_id, chunk_index)` unique constraint
 *      conflicts.
 *   1. **Stream-download the body to a temp file** via [[BillTextDownloader.downloadToTempFile]] — bytes flow through
 *      `fs2.io.file.Files.writeAll` rather than into a heap-buffered `String`. The Resource auto-deletes the temp file
 *      on close.
 *   1. **Extract plain text from the temp file** via the injected `extractText` function (production wiring uses
 *      [[repcheck.ingestion.bills.text.extraction.BillTextExtractor.extract]] which dispatches by format to Jsoup,
 *      scala-xml, PDFBox, or plain UTF-8 read). The temp-file Resource is **closed immediately after extraction
 *      returns** — chunking, embedding, and INSERTing run against the in-heap `String` only, so disk pressure windows
 *      match extraction time (seconds), not full processing time (minutes).
 *   1. **Chunk + embed + INSERT** in a per-batch loop: chunks are grouped into `embedBatchSize` batches (preserves the
 *      Ollama batching throughput win from PR #71), each batch's embeddings are computed, and each chunk is INSERTed
 *      individually inside its own auto-committing transaction. Heap stays bounded by one batch's worth of chunks +
 *      embeddings (~800 KB at default config) instead of the old all-chunks-then-all-embeddings buffer (50+ MB on a
 *      large STATUTE PDF).
 *   1. **Mark the version complete** via UPDATE `bill_text_versions SET fetched_at = NOW()`. After this, the next
 *      pipeline tick's skip-check will short-circuit re-processing.
 *   1. **Publish the ingested event** for downstream consumers (LLM analysis pipeline).
 *
 * Crash semantics: any failure between the version-row INSERT and the `markFetched` UPDATE leaves the row with
 * `fetched_at = NULL` and possibly a partial chunk list. The next pipeline tick treats it as "not yet processed",
 * clears partial chunks, and re-streams from scratch. This trades single-transaction atomicity for unbounded heap usage
 * — acceptable because the embedding model already takes 5+ seconds per batch, so a long-held tx would be holding
 * connection locks for 17+ hours on a 12,500-chunk bill.
 */
class BillTextProcessor[F[_]: Async] private[text] (
  downloader: BillTextDownloader[F],
  billRepository: BillRepository[ConnectionIO],
  textVersionRepository: BillTextVersionRepository[ConnectionIO],
  rawBillTextRepository: RawBillTextRepository[ConnectionIO],
  embeddingService: EmbeddingService[F],
  embeddingConfig: EmbeddingConfig,
  eventPublisher: IngestionEventPublisher[F],
  xa: Transactor[F],
  logger: PipelineLogger[F],
  extractText: (Path, String) => F[String],
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
      _ <- streamDownloadAndIngestChunks(
        event = event,
        dbBillId = dbBillId,
        versionId = versionId,
        correlationId = correlationId,
        logCtx = logCtx,
      )
      _ <- markVersionFetched(versionId)
      _ <- publishEvent(event, correlationId)
      _ <- logger.info(
        logCtx,
        s"Successfully processed bill text for ${event.naturalKey} — version $versionId",
      )
    } yield ProcessingResult.Succeeded(event.naturalKey, eventEmitted = true)
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
   * version already had its chunks deleted-and-re-inserted under `replaceAll` semantics, and a never-attempted version
   * has no chunks at all. Any orphans here come from a previous run that crashed between the version-row INSERT and
   * `markFetched` — clearing them prevents the per-chunk INSERTs from hitting the `(version_id, chunk_index)` unique
   * constraint.
   */
  private[pipeline] def clearOrphanChunks(versionId: Long): F[Unit] =
    TransactionRunner.run(xa)(rawBillTextRepository.deleteByVersionId(versionId))

  /**
   * Streaming-to-disk download → on-disk extraction → chunk → embed-batches → per-chunk INSERT.
   *
   * Temp-file lifetime is **scoped tightly to the extraction phase only**. The `downloadToTempFile` Resource is closed
   * (and the temp file deleted) the moment `extractText` returns the parsed `String` — chunking, embedding, and the
   * per-chunk INSERT loop run afterward against the in-heap `rawText`, never touching disk. This matters because
   * embedding is by far the slowest phase (5+ s per batch × tens-to-hundreds of batches per large bill), so scoping the
   * temp file to extraction shrinks its on-disk window from minutes-per-bill down to seconds-per-bill. Disk pressure
   * across concurrent pipeline workers stays bounded by `parallelism × peak-extraction-time`, not `parallelism ×
   * full-processing-time`.
   *
   * Failure-mode invariants are preserved: the Resource still auto-deletes on extraction failure, and a downstream
   * embedding/INSERT failure leaves no orphan file (the Resource has already released).
   */
  private[pipeline] def streamDownloadAndIngestChunks(
    event: BillTextAvailableEvent,
    dbBillId: Long,
    versionId: Long,
    correlationId: UUID,
    logCtx: LogContext,
  ): F[Unit] =
    for {
      rawText <- downloader
        .downloadToTempFile(event.textUrl, event.textFormat, correlationId)
        .use(tempPath => extractText(tempPath, event.textFormat))
      chunks <- chunkText(rawText)
      _ <- logger.info(
        logCtx,
        s"Chunked ${rawText.length}-char body into ${chunks.size} chunk(s) (max ${embeddingConfig.maxChunkChars} chars each)",
      )
      _ <- streamChunksToDb(dbBillId, versionId, chunks)
    } yield ()

  /**
   * Chunk the extracted body via [[BillTextChunker]] inside `Async[F].delay` so any thrown `InvalidChunkSize`
   * (misconfigured `OLLAMA_MAX_CHUNK_CHARS`) surfaces through the F effect's error channel instead of propagating as a
   * synchronous throw.
   */
  private[pipeline] def chunkText(content: String): F[List[String]] =
    if (embeddingConfig.maxChunkChars <= 0) Async[F].raiseError(InvalidChunkSize(embeddingConfig.maxChunkChars))
    else
      Async[F].delay {
        // Strip null bytes — Postgres TEXT can't hold them and Congress.gov occasionally serves
        // bills with stray   in the rendered HTML (carryover from the previous monolithic
        // path's defensive scrub).
        val sanitized = content.replace(" ", "")
        BillTextChunker.chunk(sanitized, embeddingConfig.maxChunkChars)
      }

  /**
   * Per-batch embed → per-chunk INSERT loop. Chunks are grouped into batches of `embeddingConfig.embedBatchSize` (50 by
   * default) so the embedding service can saturate the GPU per call (PR #71 follow-up: ~24% throughput win on the 0.6B
   * model from batching). Within each batch, after embeddings come back, each chunk is INSERTed in its own
   * auto-committing transaction so heap stays bounded.
   *
   * `globalChunkIndex` tracks the chunk_index column across batches — the chunker returns chunks in document order and
   * the index must reflect that order so `ORDER BY chunk_index` reconstructs the original. Within a batch the same
   * order is preserved by the embedding service contract (`generateEmbeddings([t1, t2, t3])` returns `[Some(v1),
   * Some(v2), Some(v3)]` aligned by index).
   *
   * `flatTraverse` (sequential) is used rather than `parTraverse` because (a) the GPU is already saturated by single
   * batch calls so concurrent calls just queue, and (b) we want strict order on the INSERT side so that a partial
   * failure halts cleanly with `chunks[0..N]` persisted in DB and the next attempt's DELETE+restream proceeds
   * correctly.
   */
  private[pipeline] def streamChunksToDb(
    dbBillId: Long,
    versionId: Long,
    chunks: List[String],
  ): F[Unit] =
    chunks.grouped(embeddingConfig.embedBatchSize).toList.zipWithIndex.traverse_ {
      case (batch, batchIdx) =>
        val baseIndex = batchIdx * embeddingConfig.embedBatchSize
        for {
          embeddings <- embeddingService.generateEmbeddings(batch)
          _          <- insertBatchOneAtATime(dbBillId, versionId, baseIndex, batch, embeddings)
        } yield ()
    }

  private[pipeline] def insertBatchOneAtATime(
    dbBillId: Long,
    versionId: Long,
    baseIndex: Int,
    batch: List[String],
    embeddings: List[Option[Array[Float]]],
  ): F[Unit] =
    batch.zip(embeddings).zipWithIndex.traverse_ {
      case ((text, embedding), localIdx) =>
        val row = RawBillTextDO(
          id = 0L,
          billId = dbBillId,
          versionId = Some(versionId),
          chunkIndex = baseIndex + localIdx,
          content = text,
          embedding = embedding,
          createdAt = None,
        )
        TransactionRunner.run(xa)(rawBillTextRepository.insertOne(row))
    }

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
