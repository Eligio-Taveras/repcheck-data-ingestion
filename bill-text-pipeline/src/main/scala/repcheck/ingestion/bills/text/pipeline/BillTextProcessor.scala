package repcheck.ingestion.bills.text.pipeline

import java.time.Instant
import java.util.UUID

import cats.effect.Async
import cats.syntax.all._

import doobie._

import repcheck.ingestion.bills.common.persistence.{BillRepository, BillTextVersionRepository, TransactionRunner}
import repcheck.ingestion.bills.text.download.BillTextDownloader
import repcheck.ingestion.bills.text.embedding.{EmbeddingGenerationFailed, EmbeddingService}
import repcheck.ingestion.bills.text.errors.{BillNotFoundForText, BillTextProcessingFailed}
import repcheck.ingestion.common.events.IngestionEventPublisher
import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}
import repcheck.pipeline.models.events.{BillTextAvailableEvent, BillTextIngestedEvent}
import repcheck.pipeline.models.metadata.ProcessingResult
import repcheck.shared.models.congress.common.FormatType
import repcheck.shared.models.congress.dos.bill.BillTextVersionDO

class BillTextProcessor[F[_]: Async] private[text] (
  downloader: BillTextDownloader[F],
  billRepository: BillRepository[ConnectionIO],
  textVersionRepository: BillTextVersionRepository[ConnectionIO],
  embeddingService: EmbeddingService[F],
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
      _         <- logger.info(logCtx, s"Processing bill text for ${event.naturalKey} (format=${event.textFormat})")
      dbBillId  <- lookupBillId(event.naturalKey)
      content   <- downloadText(event, correlationId)
      embedding <- embeddingService.generateEmbedding(content)
      version   <- buildTextVersion(event, dbBillId, content, embedding)
      _         <- storeVersion(version)
      _         <- publishEvent(event, correlationId)
      _         <- logger.info(logCtx, s"Successfully processed bill text for ${event.naturalKey}")
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

  private[pipeline] def buildTextVersion(
    event: BillTextAvailableEvent,
    dbBillId: Long,
    content: String,
    embedding: Option[Array[Float]],
  ): F[BillTextVersionDO] = {
    val formatType = FormatType.fromString(event.textFormat).toOption
    Async[F].delay {
      BillTextVersionDO(
        id = 0L,
        billId = dbBillId,
        versionCode = event.versionCode,
        versionType = event.textFormat,
        versionDate = None,
        formatType = formatType,
        url = Some(event.textUrl),
        content = Some(content),
        embedding = embedding,
        fetchedAt = Some(Instant.now()),
        createdAt = None,
      )
    }
  }

  private[pipeline] def storeVersion(
    version: BillTextVersionDO
  ): F[Long] =
    TransactionRunner.run(xa)(textVersionRepository.storeAndUpdateBill(version))

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
      case _: BillNotFoundForText             => "Systemic"
      case _: BillTextProcessingFailed        => "Systemic"
      case _: EmbeddingGenerationFailed       => "Transient"
      case _: java.net.SocketTimeoutException => "Transient"
      case _: java.net.ConnectException       => "Transient"
      case _: java.io.IOException             => "Transient"
      case _: java.sql.SQLTransientException  => "Transient"
      case _                                  => "Systemic"
    }

}
