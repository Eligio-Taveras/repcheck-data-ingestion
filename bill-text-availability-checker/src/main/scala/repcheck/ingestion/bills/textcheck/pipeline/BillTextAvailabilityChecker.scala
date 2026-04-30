package repcheck.ingestion.bills.textcheck.pipeline

import java.util.UUID

import cats.effect.Async
import cats.syntax.all._

import fs2.Stream

import doobie.ConnectionIO
import doobie.util.transactor.Transactor

import repcheck.ingestion.bills.common.persistence.{BillRepository, TransactionRunner}
import repcheck.ingestion.bills.textcheck.api.BillTextApiClient
import repcheck.ingestion.bills.textcheck.config.BillTextCheckerConfig
import repcheck.ingestion.bills.textcheck.errors.{BillTextCheckFailed, EventPublishErrorClassifier}
import repcheck.ingestion.bills.textcheck.selection.TextVersionSelector
import repcheck.ingestion.common.events.IngestionEventPublisher
import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}
import repcheck.pipeline.models.errors.RetryWrapper
import repcheck.pipeline.models.events.BillTextAvailableEvent
import repcheck.pipeline.models.metadata.ProcessingResult
import repcheck.shared.models.congress.dos.bill.BillDO

class BillTextAvailabilityChecker[F[_]: Async](
  textApiClient: BillTextApiClient[F],
  billRepo: BillRepository[ConnectionIO],
  eventPublisher: IngestionEventPublisher[F],
  retryWrapper: RetryWrapper[F],
  xa: Transactor[F],
  config: BillTextCheckerConfig,
  logger: PipelineLogger[F],
) {

  private val StepName = "bill-text-availability-check"

  def checkAll(runId: Long): Stream[F, ProcessingResult] = {
    val logCtx     = LogContext(runId = runId.toString, stepName = StepName)
    val congresses = config.congressList

    Stream
      .eval(
        logger.info(
          logCtx,
          s"Starting bill text availability check (congresses=${
              if (congresses.isEmpty) "<all>" else congresses.mkString(",")
            })",
        ) *>
          TransactionRunner.run(xa)(billRepo.findBillsNeedingTextCheck(congresses))
      )
      .flatMap { bills =>
        Stream
          .eval(logger.info(logCtx, s"Found ${bills.size} bills needing text check"))
          .drain ++
          Stream
            .emits(bills)
            .parEvalMap(config.parallelism)(bill => checkBill(bill, UUID.randomUUID()))
      }
  }

  private[pipeline] def checkBill(bill: BillDO, correlationId: UUID): F[ProcessingResult] = {
    val billId = bill.naturalKey
    val logCtx = LogContext(
      runId = correlationId.toString,
      stepName = StepName,
      correlationId = Some(correlationId),
      entityId = Some(billId),
    )

    val check: F[ProcessingResult] = for {
      selected <- fetchBestVersion(bill)
      result   <- evaluateAndEmit(selected, bill, correlationId, logCtx)
    } yield result

    check.handleErrorWith(error => handleCheckError(billId, error, logCtx))
  }

  private[pipeline] def fetchBestVersion(bill: BillDO): F[Option[TextVersionSelector.SelectedVersion]] =
    textApiClient
      .fetchTextVersions(
        congress = bill.congress,
        billType = bill.billType.apiValue,
        number = bill.number,
      )
      .map(TextVersionSelector.selectBestVersion)

  private[pipeline] def evaluateAndEmit(
    selected: Option[TextVersionSelector.SelectedVersion],
    bill: BillDO,
    correlationId: UUID,
    logCtx: LogContext,
  ): F[ProcessingResult] = {
    val billId = bill.naturalKey
    selected match {
      case None =>
        logger
          .debug(logCtx, s"No text versions found for bill $billId")
          .as(ProcessingResult.Skipped(entityId = billId, reason = "No text versions available"))

      case Some(sv) =>
        sv.versionType match {
          case None =>
            Async[F].pure(
              ProcessingResult.Failed(
                entityId = billId,
                reason = "Selected text version has no version code",
                errorClass = "MissingVersionCode",
              )
            )
          case Some(versionCode) =>
            evaluateChange(bill, sv, versionCode, correlationId, logCtx)
        }
    }
  }

  private[pipeline] def evaluateChange(
    bill: BillDO,
    sv: TextVersionSelector.SelectedVersion,
    versionCode: String,
    correlationId: UUID,
    logCtx: LogContext,
  ): F[ProcessingResult] = {
    val billId  = bill.naturalKey
    val isNew   = bill.textVersionType.isEmpty
    val changed = bill.textVersionType.exists(stored => stored.toString =!= versionCode)

    if (!isNew && !changed) {
      logger
        .debug(logCtx, s"Text unchanged for bill $billId")
        .as(ProcessingResult.Skipped(entityId = billId, reason = "Text version unchanged"))
    } else {
      emitEvent(bill, sv, versionCode, correlationId, logCtx)
    }
  }

  private[pipeline] def emitEvent(
    bill: BillDO,
    sv: TextVersionSelector.SelectedVersion,
    versionCode: String,
    correlationId: UUID,
    logCtx: LogContext,
  ): F[ProcessingResult] = {
    val billId              = bill.naturalKey
    val previousVersionCode = bill.textVersionType.map(_.toString)
    val event = BillTextAvailableEvent(
      naturalKey = billId,
      congress = bill.congress,
      textUrl = sv.url,
      textFormat = sv.formatType,
      versionCode = versionCode,
      previousVersionCode = previousVersionCode,
    )
    retryWrapper.withRetry(
      operation = eventPublisher.billTextAvailable(event, correlationId),
      config = config.eventPublishRetry,
      classifier = EventPublishErrorClassifier,
      errorFactory = (msg, cause) => BillTextCheckFailed(billId, msg, cause),
      correlationId = correlationId,
    ) *>
      logger.info(logCtx, s"Emitted BillTextAvailableEvent for bill $billId (version=$versionCode)") *>
      Async[F].pure(ProcessingResult.Succeeded(entityId = billId, eventEmitted = true))
  }

  private[pipeline] def handleCheckError(
    billId: String,
    error: Throwable,
    logCtx: LogContext,
  ): F[ProcessingResult] =
    logger
      .error(
        logCtx,
        s"Failed to check text availability for bill $billId: ${error.getMessage}",
        Some(error),
      )
      .as(
        ProcessingResult.Failed(
          entityId = billId,
          reason = s"Text check failed: ${error.getMessage}",
          errorClass = "BillTextCheckFailed",
        )
      )

}
