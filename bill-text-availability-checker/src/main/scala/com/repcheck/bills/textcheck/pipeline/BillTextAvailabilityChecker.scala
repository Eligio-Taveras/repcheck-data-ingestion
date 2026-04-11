package com.repcheck.bills.textcheck.pipeline

import java.util.UUID

import cats.effect.Async
import cats.syntax.all._

import doobie.ConnectionIO
import doobie.util.transactor.Transactor

import fs2.Stream

import repcheck.ingestion.common.events.IngestionEventPublisher
import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}
import repcheck.pipeline.models.events.BillTextAvailableEvent
import repcheck.pipeline.models.metadata.ProcessingResult
import repcheck.shared.models.congress.dos.bill.BillDO

import com.repcheck.bills.common.persistence.{BillRepository, TransactionRunner}
import com.repcheck.bills.textcheck.api.BillTextApiClient
import com.repcheck.bills.textcheck.config.BillTextCheckerConfig
import com.repcheck.bills.textcheck.selection.TextVersionSelector

class BillTextAvailabilityChecker[F[_]: Async](
  textApiClient: BillTextApiClient[F],
  billRepo: BillRepository[ConnectionIO],
  eventPublisher: IngestionEventPublisher[F],
  xa: Transactor[F],
  config: BillTextCheckerConfig,
  logger: PipelineLogger[F],
) {

  private val StepName = "bill-text-availability-check"

  def checkAll(correlationId: UUID): Stream[F, ProcessingResult] = {
    val logCtx = LogContext(runId = correlationId.toString, stepName = StepName)

    Stream
      .eval(
        logger.info(logCtx, "Starting bill text availability check") *>
          TransactionRunner.run(xa)(billRepo.findBillsNeedingTextCheck())
      )
      .flatMap { bills =>
        Stream
          .eval(logger.info(logCtx, s"Found ${bills.size} bills needing text check"))
          .drain ++
          Stream
            .emits(bills)
            .parEvalMap(config.parallelism) { bill =>
              checkBill(bill, correlationId)
            }
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
      versions <- textApiClient.fetchTextVersions(
        congress = bill.congress,
        billType = bill.billType.apiValue,
        number = bill.number,
      )
      selected = TextVersionSelector.selectBestVersion(versions)
      result <- selected match {
        case None =>
          logger
            .debug(logCtx, s"No text versions found for bill $billId")
            .as(ProcessingResult.Skipped(entityId = billId, reason = "No text versions available"))

        case Some(sv) =>
          val isNew = bill.textVersionType.isEmpty
          val versionCodeChanged = bill.textVersionType.exists { stored =>
            sv.versionType.forall(newCode => stored.toString =!= newCode)
          }

          if (!isNew && !versionCodeChanged) {
            logger
              .debug(logCtx, s"Text unchanged for bill $billId")
              .as(ProcessingResult.Skipped(entityId = billId, reason = "Text version unchanged"))
          } else {
            val previousVersionCode = bill.textVersionType.map(_.toString)
            val event = BillTextAvailableEvent(
              billId = billId,
              congress = bill.congress,
              textUrl = sv.url,
              textFormat = sv.formatType,
              versionCode = sv.versionType.getOrElse("UNKNOWN"),
              previousVersionCode = previousVersionCode,
            )
            eventPublisher.billTextAvailable(event, correlationId) *>
              logger.info(
                logCtx,
                s"Emitted BillTextAvailableEvent for bill $billId (version=${sv.versionType.getOrElse("UNKNOWN")})",
              ) *>
              Async[F].pure(ProcessingResult.Succeeded(entityId = billId, eventEmitted = true))
          }
      }
    } yield result

    check.handleErrorWith { error =>
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
  }

}
