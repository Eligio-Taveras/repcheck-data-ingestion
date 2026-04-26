package repcheck.ingestion.bills.text.pipeline

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
 * Processes one `BillTextAvailableEvent` end-to-end (download → chunk → embed-each → persist version metadata + N
 * chunks → publish ingested event). Composes the version-row upsert and the chunk delete-then-insert under a single
 * transaction so observers never see a half-written intermediate state. See db-migrations 026 + shared-models 0.1.34
 * for the post-P6.H4 storage shape.
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
              s"Skipping ${event.naturalKey} version=${event.versionCode} — bill_text_versions row already exists (Pub/Sub redelivery or upstream double-emission)",
            )
            .as(ProcessingResult.Skipped(event.naturalKey, "already-processed"))
        } else {
          processFreshBillText(event, dbBillId, correlationId, logCtx)
        }
    } yield result

  /**
   * Skip-check before doing the expensive download → chunk → embed → persist work. Returns true iff a
   * `bill_text_versions` row already exists for `(billId, versionCode)`. Guards against two main re-process cases:
   *   1. Pub/Sub redelivery — the previous attempt's ack didn't land or the message was redelivered after a failure,
   *      but the persistence transaction had already committed 2. Upstream double-emission —
   *      bill-text-availability-checker ran twice on the same bill+version (rare, but possible with concurrent ofelia
   *      ticks before the `text_url IS NULL` filter took effect)
   *
   * The check is at the (billId, versionCode) tuple level so a re-versioning of the same bill (e.g., the IH version was
   * processed but the bill has now progressed to RH) still triggers fresh processing.
   */
  private[pipeline] def isAlreadyProcessed(billId: Long, versionCode: String): F[Boolean] =
    TransactionRunner
      .run(xa)(textVersionRepository.findByBillId(billId))
      .map(_.exists(_.versionCode == versionCode))

  private[pipeline] def processFreshBillText(
    event: BillTextAvailableEvent,
    dbBillId: Long,
    correlationId: UUID,
    logCtx: LogContext,
  ): F[ProcessingResult] =
    for {
      content <- downloadText(event, correlationId)
      chunks  <- chunkText(content)
      _ <- logger.info(
        logCtx,
        s"Chunked ${content.length}-char body into ${chunks.size} chunk(s) (max ${embeddingConfig.maxChunkChars} chars each)",
      )
      embeddings <- embedAllChunks(chunks)
      version = buildTextVersion(event, dbBillId)
      versionId <- persistVersionAndChunks(version, dbBillId, chunks, embeddings)
      _         <- publishEvent(event, correlationId)
      _ <- logger.info(
        logCtx,
        s"Successfully processed bill text for ${event.naturalKey} — version $versionId, ${chunks.size} chunk(s)",
      )
    } yield ProcessingResult.Succeeded(event.naturalKey, eventEmitted = true)

  private[pipeline] def lookupBillId(billNaturalKey: String): F[Long] =
    TransactionRunner.run(xa)(billRepository.findByBillId(billNaturalKey)).flatMap {
      case Some(bill) => Async[F].pure(bill.billId)
      case None       => Async[F].raiseError(BillNotFoundForText(billNaturalKey))
    }

  private[pipeline] def downloadText(
    event: BillTextAvailableEvent,
    correlationId: UUID,
  ): F[String] =
    downloader.download(event.textUrl, event.textFormat, correlationId)

  /**
   * Chunk the downloaded body via [[BillTextChunker]] inside `Async[F].delay` so any thrown `InvalidChunkSize`
   * (misconfigured `OLLAMA_MAX_CHUNK_CHARS`) surfaces through the F effect's error channel instead of propagating as a
   * synchronous throw.
   */
  private[pipeline] def chunkText(content: String): F[List[String]] =
    if (embeddingConfig.maxChunkChars <= 0) Async[F].raiseError(InvalidChunkSize(embeddingConfig.maxChunkChars))
    else
      Async[F].delay {
        // Strip null bytes — Postgres TEXT can't hold them and Congress.gov occasionally serves
        // bills with stray \u0000 in the rendered HTML (carryover from the previous monolithic
        // path's defensive scrub).
        val sanitized = content.replace("\u0000", "")
        BillTextChunker.chunk(sanitized, embeddingConfig.maxChunkChars)
      }

  /**
   * Embed all chunks via batched calls to the embedding service. Chunks are grouped into batches of size
   * `embeddingConfig.embedBatchSize` and each batch is sent as a single `/api/embed` array call (see
   * [[EmbeddingService.generateEmbeddings]]). Empirically this captures most of the available batching gain (~15% on
   * the 4B model, ~24% on the 0.6B model) without the overhead of pushing the GPU past saturation — see PR #71's
   * follow-up benchmark for the curve.
   *
   * Batches run sequentially (`flatTraverse`) rather than in parallel because the GPU is already the bottleneck:
   * concurrent in-flight requests against a saturated device just queue up at the model with worse failure-mode
   * isolation (a single 5xx can affect a wider blast radius). `EmbeddingService.generateEmbeddings` swallows transient
   * batch failures into `List.fill(N)(None)`, so a partial-embedding result still produces a complete chunk list with
   * some `embedding = None` rows — the next pipeline tick can re-process.
   */
  private[pipeline] def embedAllChunks(chunks: List[String]): F[List[Option[Array[Float]]]] =
    chunks.grouped(embeddingConfig.embedBatchSize).toList.flatTraverse(embeddingService.generateEmbeddings)

  private[pipeline] def buildTextVersion(
    event: BillTextAvailableEvent,
    dbBillId: Long,
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
      fetchedAt = Some(Instant.now()),
      createdAt = None,
    )
  }

  /**
   * Single-transaction persistence: upsert the parent `bill_text_versions` row, then delete-and-insert all chunks for
   * that version. Composing both in one `ConnectionIO` means a downstream failure (chunk batch insert errors,
   * connection drop) rolls back the parent version row too — readers never see a metadata row pointing at zero chunks.
   */
  private[pipeline] def persistVersionAndChunks(
    version: BillTextVersionDO,
    dbBillId: Long,
    chunks: List[String],
    embeddings: List[Option[Array[Float]]],
  ): F[Long] = {
    val txn: ConnectionIO[Long] =
      for {
        versionId <- textVersionRepository.storeAndUpdateBill(version)
        rows = buildChunkRows(dbBillId, versionId, chunks, embeddings)
        _ <- rawBillTextRepository.replaceAll(versionId, rows)
      } yield versionId
    TransactionRunner.run(xa)(txn)
  }

  private[pipeline] def buildChunkRows(
    dbBillId: Long,
    versionId: Long,
    chunks: List[String],
    embeddings: List[Option[Array[Float]]],
  ): List[RawBillTextDO] =
    chunks.zip(embeddings).zipWithIndex.map {
      case ((chunk, embedding), idx) =>
        RawBillTextDO(
          id = 0L,
          billId = dbBillId,
          versionId = Some(versionId),
          chunkIndex = idx,
          content = chunk,
          embedding = embedding,
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
