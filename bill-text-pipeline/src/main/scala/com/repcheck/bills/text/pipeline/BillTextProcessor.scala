package com.repcheck.bills.text.pipeline

import java.time.Instant
import java.util.UUID

import cats.effect.Async
import cats.syntax.all._

import doobie._

import repcheck.ingestion.common.events.IngestionEventPublisher
import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}
import repcheck.pipeline.models.events.{BillTextAvailableEvent, BillTextIngestedEvent}
import repcheck.pipeline.models.metadata.ProcessingResult
import repcheck.shared.models.congress.common.FormatType
import repcheck.shared.models.congress.dos.bill.BillTextVersionDO

import com.repcheck.bills.common.persistence.{BillTextVersionRepository, TransactionRunner}
import com.repcheck.bills.text.download.BillTextDownloader
import com.repcheck.bills.text.errors.BillTextProcessingFailed

class BillTextProcessor[F[_]: Async] private[pipeline] (
  downloader: BillTextDownloader[F],
  repository: BillTextVersionRepository[ConnectionIO],
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
      entityId = Some(event.billId),
    )

    processEventInternal(event, correlationId, logCtx).handleErrorWith { error =>
      val errorClass = classifyError(error)
      logger.error(logCtx, s"Failed to process bill text for ${event.billId}: ${error.getMessage}", Some(error)) *>
        Async[F].pure(ProcessingResult.Failed(event.billId, error.getMessage, errorClass))
    }
  }

  private[pipeline] def processEventInternal(
    event: BillTextAvailableEvent,
    correlationId: UUID,
    logCtx: LogContext,
  ): F[ProcessingResult] =
    for {
      _       <- logger.info(logCtx, s"Processing bill text for ${event.billId} (format=${event.textFormat})")
      content <- downloadText(event, correlationId)
      version <- buildVersion(event, content)
      _       <- storeVersion(version)
      _       <- publishEvent(event, correlationId)
      _       <- logger.info(logCtx, s"Successfully processed bill text for ${event.billId}")
    } yield ProcessingResult.Succeeded(event.billId, eventEmitted = true)

  private[pipeline] def downloadText(
    event: BillTextAvailableEvent,
    correlationId: UUID,
  ): F[String] =
    downloader.download(event.textUrl, event.textFormat, correlationId)

  private[pipeline] def buildVersion(
    event: BillTextAvailableEvent,
    content: String,
  ): F[BillTextVersionDO] = {
    val formatType = FormatType.fromString(event.textFormat).toOption
    Async[F].delay {
      BillTextVersionDO(
        id = 0L,
        billId = 0L,
        versionCode = event.versionCode,
        versionType = event.textFormat,
        versionDate = None,
        formatType = formatType,
        url = Some(event.textUrl),
        content = Some(content),
        embedding = None,
        fetchedAt = Some(Instant.now()),
        createdAt = None,
      )
    }
  }

  private[pipeline] def storeVersion(
    version: BillTextVersionDO
  ): F[Long] =
    TransactionRunner.run(xa)(repository.insertVersion(version))

  private[pipeline] def publishEvent(
    event: BillTextAvailableEvent,
    correlationId: UUID,
  ): F[String] = {
    val ingestedEvent = BillTextIngestedEvent(
      billId = event.billId,
      versionId = correlationId,
      congress = event.congress,
      versionCode = event.versionCode,
      previousVersionCode = event.previousVersionCode,
      committeeCode = None,
    )
    eventPublisher.billTextIngested(ingestedEvent, correlationId)
  }

  private[pipeline] def classifyError(error: Throwable): String =
    error match {
      case _: BillTextProcessingFailed        => "Systemic"
      case _: java.net.SocketTimeoutException => "Transient"
      case _: java.net.ConnectException       => "Transient"
      case _: java.io.IOException             => "Transient"
      case _: java.sql.SQLTransientException  => "Transient"
      case _                                  => "Systemic"
    }

}
