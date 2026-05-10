package repcheck.ingestion.bills.text.pipeline

import java.util.UUID

import cats.effect.Async
import cats.syntax.all._

import fs2.Stream

import doobie._

import repcheck.ingestion.bills.common.persistence.{BillRepository, BillTextVersionRepository, TransactionRunner}
import repcheck.ingestion.bills.text.download.BillTextDownloader
import repcheck.ingestion.bills.text.embedding.{BillChunkEmbedder, BillEmbedCtx}
import repcheck.ingestion.bills.text.errors.{BillNotFoundForText, BillTextProcessingFailed}
import repcheck.ingestion.common.events.IngestionEventPublisher
import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}
import repcheck.ingestion.text.chunking.TextChunker
import repcheck.ingestion.text.embedding.{EmbeddingConfig, EmbeddingContextLengthExceeded, EmbeddingGenerationFailed}
import repcheck.pipeline.models.events.{BillTextAvailableEvent, BillTextIngestedEvent}
import repcheck.pipeline.models.metadata.ProcessingResult
import repcheck.shared.models.congress.common.FormatType
import repcheck.shared.models.congress.dos.bill.BillTextVersionDO

/**
 * Processes one `BillTextAvailableEvent` end-to-end. Post-Option-C-refactor, this processor's job is narrowed to:
 *
 *   1. **Skip-check** the version row (`fetched_at IS NOT NULL` → already done).
 *   1. **Insert version row** with `fetched_at = NULL` so a crash mid-flight is reprocessable.
 *   1. **Hand off** to the cross-bill embedder via `submit(ctx, chunkStream, ackId, ack, nack)` and return immediately.
 *
 * The embedder owns the rest: chunk persistence (idempotent UPSERT — no orphan-clear needed), trim past the new
 * submission's tail, `markFetched`, publishing the BillTextIngestedEvent, and ACK / NACK on the Pub/Sub message. The
 * `ack` callback handed into `embedder.submit` composes `publishIngestedEvent *> subscriber.acknowledge(ackId)` so a
 * publish failure surfaces as a NACK (and the next redelivery's UPSERT is idempotent — no duplicate work).
 *
 * ==Crash semantics==
 *
 * Any failure between the version-row INSERT and the embedder's eventual `markFetched` leaves the row with `fetched_at
 * \= NULL` and possibly a partial chunk list. The next pipeline tick treats it as "not yet processed"; UPSERT
 * overwrites overlapping chunks; trim removes any stale tail; markFetched flips `fetched_at` to NOW(). No separate
 * `clearOrphanChunks` step is needed — the previous explicit DELETE-before-INSERT pattern is replaced by idempotent
 * UPSERT-then-trim, which is also race-safe under concurrent redelivery (the previous DELETE was a hazard if two
 * producers' chunks raced).
 *
 * ==Why no version-date gate==
 *
 * `BillTextAvailableEvent` does not carry a `versionDate` field, and `bill_text_versions.version_date` is always
 * written as `None` today. The amendments-side processor uses a version-date-gated UPSERT to filter older redeliveries
 * at the DB layer; bills cannot, so it falls back to last-writer-wins. Safe in practice because Congress.gov text for a
 * given `(billId, versionCode)` is monotonic.
 */
class BillTextProcessor[F[_]: Async] private[text] (
  downloader: BillTextDownloader[F],
  billRepository: BillRepository[ConnectionIO],
  textVersionRepository: BillTextVersionRepository[ConnectionIO],
  embedder: BillChunkEmbedder[F],
  embeddingConfig: EmbeddingConfig,
  eventPublisher: IngestionEventPublisher[F],
  xa: Transactor[F],
  logger: PipelineLogger[F],
  extractText: (Stream[F, Byte], String) => Stream[F, String],
) {

  private val StepName = "bill-text-processing"

  /**
   * Submit one Pub/Sub message for processing.
   *
   * `ack` and `nack` are the message's acknowledgement effects, supplied by the calling subscriber loop. The processor
   * invokes `nack` on its own failures (bill-not-found, version-row UPSERT raised). On successful submission the
   * processor hands `ack` (composed with `publishIngestedEvent`) and `nack` to the embedder, which fires whichever is
   * appropriate once the chunks have been processed (or failed).
   *
   * Returned `ProcessingResult` is for run-summary aggregation only — actual completion happens later inside the
   * embedder's per-ackId state machine.
   */
  def processEvent(
    event: BillTextAvailableEvent,
    correlationId: UUID,
    ackId: String,
    ack: F[Unit],
    nack: F[Unit],
  ): F[ProcessingResult] = {
    val logCtx = LogContext(
      runId = correlationId.toString,
      stepName = StepName,
      correlationId = Some(correlationId),
      entityId = Some(event.naturalKey),
    )

    processEventInternal(event, correlationId, ackId, ack, nack, logCtx).handleErrorWith { error =>
      val errorClass = classifyError(error)
      logger.error(logCtx, s"Failed to process bill text for ${event.naturalKey}: ${error.getMessage}", Some(error)) *>
        nack *>
        Async[F].pure(ProcessingResult.Failed(event.naturalKey, error.getMessage, errorClass))
    }
  }

  private[pipeline] def processEventInternal(
    event: BillTextAvailableEvent,
    correlationId: UUID,
    ackId: String,
    ack: F[Unit],
    nack: F[Unit],
    logCtx: LogContext,
  ): F[ProcessingResult] =
    for {
      _        <- logger.info(logCtx, s"Processing bill text for ${event.naturalKey} (format=${event.textFormat})")
      dbBillId <- lookupBillId(event.naturalKey)
      alreadyProcessed <- isAlreadyProcessed(dbBillId, event.versionCode)
      result <-
        if (alreadyProcessed) {
          // Re-publish on the skip path. Without this, a previous run that succeeded internally (DB updated +
          // markFetched ran) but failed on `publishEvent` would leave the BillTextIngestedEvent permanently dropped:
          // the NACK would trigger redelivery, and the redelivery would skip-and-ACK without re-publishing. Pub/Sub's
          // at-least-once contract requires downstream consumers to be idempotent for the BillTextIngestedEvent, so
          // re-publishing on every skip is safe — duplicate downstream processing is the expected at-least-once shape.
          logger
            .info(
              logCtx,
              s"Skipping ${event.naturalKey} version=${event.versionCode} — bill_text_versions row already complete (fetched_at IS NOT NULL); re-publishing BillTextIngestedEvent then ACKing",
            ) *>
            publishEvent(event, correlationId).void *>
            ack.as(ProcessingResult.Skipped(event.naturalKey, "already-processed"))
        } else {
          processFreshBillText(event, dbBillId, correlationId, ackId, ack, nack, logCtx)
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
    ackId: String,
    ack: F[Unit],
    nack: F[Unit],
    logCtx: LogContext,
  ): F[ProcessingResult] = {
    val pendingVersion = buildTextVersion(event, dbBillId, fetchedAt = None)
    // `defer` keeps the publishEvent method-call lazy: without it, building the composedAck val would eagerly invoke
    // `eventPublisher.billTextIngested` (the IO returned IS lazy, but the method invocation itself isn't), which
    // means a mock-publisher would see a verification hit before the embedder fires its ack. Composed ack also
    // means a publish failure surfaces as a NACK via the embedder's safeAck wrapper — Pub/Sub redelivers, trim +
    // markFetched are idempotent, no duplicate work.
    val composedAck = Async[F].defer(publishEvent(event, correlationId).void) *> ack
    for {
      versionId <- persistPendingVersion(pendingVersion)
      ctx = BillEmbedCtx(dbBillId = dbBillId, versionId = versionId, naturalKey = event.naturalKey)
      _ <- streamDownloadExtractChunkAndSubmit(
        event = event,
        ctx = ctx,
        correlationId = correlationId,
        ackId = ackId,
        ack = composedAck,
        nack = nack,
      )
      _ <- logger.info(
        logCtx,
        s"Submitted bill text for ${event.naturalKey} — version $versionId; ACK delegated to embedder",
      )
    } yield ProcessingResult.Succeeded(event.naturalKey, eventEmitted = false)
  }

  private[pipeline] def lookupBillId(billNaturalKey: String): F[Long] =
    TransactionRunner.run(xa)(billRepository.findByBillId(billNaturalKey)).flatMap {
      case Some(bill) => Async[F].pure(bill.billId)
      case None       => Async[F].raiseError(BillNotFoundForText(billNaturalKey))
    }

  /**
   * Insert the parent `bill_text_versions` row in its own committed transaction. `fetched_at` is left NULL until the
   * embedder's per-ackId completion handler runs `markFetched` after persisting all chunks. The companion `bills.*`
   * columns (`text_url`, etc.) are updated atomically inside the same transaction via `storeAndUpdateBill` so the bill
   * row's pointer to the version is consistent immediately.
   */
  private[pipeline] def persistPendingVersion(version: BillTextVersionDO): F[Long] =
    TransactionRunner.run(xa)(textVersionRepository.storeAndUpdateBill(version))

  /**
   * Build the per-bill chunk stream and submit it to the cross-bill embedder. Returns once the chunks have been
   * enqueued and the residual flush is driven; the embedder owns the rest of the lifecycle (UPSERT, trim, markFetched,
   * ack/nack).
   */
  private[pipeline] def streamDownloadExtractChunkAndSubmit(
    event: BillTextAvailableEvent,
    ctx: BillEmbedCtx,
    correlationId: UUID,
    ackId: String,
    ack: F[Unit],
    nack: F[Unit],
  ): F[Unit] = {
    val bytes = downloader.streamBody(event.textUrl, event.textFormat, correlationId)
    val chunkStream = extractText(bytes, event.textFormat)
      .map(stripNullBytes)
      .filter(_.nonEmpty)
      .through(TextChunker.chunkPipe(embeddingConfig.maxChunkChars))
    embedder.submit(ctx, chunkStream, ackId, ack, nack)
  }

  /**
   * Postgres TEXT can't hold null bytes; Congress.gov occasionally serves bills with stray U+0000 chars in the rendered
   * HTML. The buffered code path scrubbed these once on the whole document; the streaming path scrubs per fragment
   * (cheaper, local).
   */
  private[pipeline] def stripNullBytes(text: String): String =
    text.filter(_.toInt != 0)

  private[pipeline] def buildTextVersion(
    event: BillTextAvailableEvent,
    dbBillId: Long,
    fetchedAt: Option[java.time.Instant],
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
      case _: EmbeddingContextLengthExceeded  => "Systemic"
      case _: EmbeddingGenerationFailed       => "Transient"
      case _: java.net.SocketTimeoutException => "Transient"
      case _: java.net.ConnectException       => "Transient"
      case _: java.io.IOException             => "Transient"
      case _: java.sql.SQLTransientException  => "Transient"
      case _                                  => "Systemic"
    }

}
