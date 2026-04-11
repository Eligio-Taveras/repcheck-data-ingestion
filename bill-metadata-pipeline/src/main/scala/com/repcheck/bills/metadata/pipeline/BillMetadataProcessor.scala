package com.repcheck.bills.metadata.pipeline

import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.UUID

import cats.effect.Async
import cats.syntax.all._

import fs2.Stream

import doobie._

import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}
import repcheck.ingestion.common.placeholders.{EntityRepository, PlaceholderCreator}
import repcheck.pipeline.models.metadata.ProcessingResult
import repcheck.shared.models.congress.dos.bill.BillDO
import repcheck.shared.models.congress.dos.member.MemberDO
import repcheck.shared.models.congress.dto.bill.{BillListItemDTO, CoSponsorDTO}
import repcheck.shared.models.congress.dto.conversions.BillConversions._

import com.repcheck.bills.common.persistence.{
  BillCosponsorRepository,
  BillHistoryArchiver,
  BillRepository,
  BillSubjectRepository,
  MemberLookupRepository,
  TransactionRunner,
}
import com.repcheck.bills.metadata.api.BillsApiClient
import com.repcheck.bills.metadata.config.BillMetadataConfig
import com.repcheck.bills.metadata.errors.BillProcessingFailed

class BillMetadataProcessor[F[_]: Async](
  apiClient: BillsApiClient[F],
  billRepo: BillRepository[ConnectionIO],
  cosponsorRepo: BillCosponsorRepository[ConnectionIO],
  subjectRepo: BillSubjectRepository[ConnectionIO],
  historyArchiver: BillHistoryArchiver[ConnectionIO],
  memberLookupRepo: MemberLookupRepository[ConnectionIO],
  placeholderCreator: PlaceholderCreator[F],
  memberEntityRepo: EntityRepository[F, MemberDO],
  xa: Transactor[F],
  config: BillMetadataConfig,
  logger: PipelineLogger[F],
) {

  private val stepName = "bill-metadata"

  private val memberResolver =
    new MemberResolver[F](memberLookupRepo, placeholderCreator, memberEntityRepo, xa, logger)

  private val billPersister =
    new BillPersister[F](billRepo, cosponsorRepo, subjectRepo, historyArchiver, xa)

  def streamAll(correlationId: UUID): Stream[F, ProcessingResult] = {
    val fromDateTime = Instant.now().minus(config.lookbackDays.toLong, ChronoUnit.DAYS)
    val params       = repcheck.ingestion.common.api.FetchParams(fromDateTime = Some(fromDateTime))

    apiClient
      .fetchAll(params)
      .parEvalMap(config.parallelism) { listItem =>
        processListItem(listItem, correlationId).handleError { e =>
          val naturalKey = buildNaturalKey(listItem)
          ProcessingResult.Failed(naturalKey, e.getMessage)
        }
      }
  }

  private[pipeline] def buildNaturalKey(listItem: BillListItemDTO): String =
    s"${listItem.congress}-${listItem.billType.toUpperCase}-${listItem.number}"

  private[pipeline] def processListItem(
    listItem: BillListItemDTO,
    correlationId: UUID,
  ): F[ProcessingResult] = {
    val naturalKey = buildNaturalKey(listItem)
    val logCtx     = LogContext(correlationId.toString, stepName, Some(correlationId), Some(naturalKey))

    for {
      stored <- TransactionRunner.run(xa)(billRepo.findByBillId(naturalKey))
      result <- evaluateAndProcess(listItem, naturalKey, stored, correlationId, logCtx)
    } yield result
  }

  private def evaluateAndProcess(
    listItem: BillListItemDTO,
    naturalKey: String,
    stored: Option[BillDO],
    correlationId: UUID,
    logCtx: LogContext,
  ): F[ProcessingResult] = {
    val incomingDate = listItem.updateDate.flatMap(s => parseInstantStr(s))
    val storedDate   = stored.flatMap(_.updateDate).flatMap(s => parseInstantStr(s))

    stored match {
      case None =>
        logger.info(logCtx, s"New bill detected: $naturalKey") *>
          processBill(listItem, naturalKey, isNew = true, stored, correlationId, logCtx)

      case Some(_) if incomingDate.exists(inc => storedDate.forall(sd => inc.isAfter(sd))) =>
        logger.info(logCtx, s"Updated bill detected: $naturalKey") *>
          processBill(listItem, naturalKey, isNew = false, stored, correlationId, logCtx)

      case Some(_) =>
        logger.debug(logCtx, s"Bill unchanged: $naturalKey") *>
          Async[F].pure(ProcessingResult.Skipped(naturalKey, "unchanged"))
    }
  }

  private[pipeline] def processBill(
    listItem: BillListItemDTO,
    naturalKey: String,
    isNew: Boolean,
    stored: Option[BillDO],
    correlationId: UUID,
    logCtx: LogContext,
  ): F[ProcessingResult] = {
    val ctx = logCtx.copy(
      correlationId = Some(correlationId),
      additional = logCtx.additional ++ stored.map(b => "storedBillId" -> b.billId.toString),
    )
    for {
      detail <- apiClient.fetchDetail(listItem.url)
      conversionResult <- Async[F].fromEither(
        detail.toDO.leftMap(reason => BillProcessingFailed(naturalKey, s"DTO-to-DO conversion failed: $reason"))
      )
      cosponsorDTOs <- fetchCosponsorsFromDetail(detail, ctx)
      billDO        <- memberResolver.ensureSponsorPlaceholder(conversionResult.bill, detail, ctx)
      cosponsorDOs  <- memberResolver.buildCosponsorDOs(cosponsorDTOs, ctx)
      _             <- billPersister.persistBill(billDO, conversionResult.subjects, cosponsorDOs, naturalKey, isNew)
      _             <- logger.info(ctx, s"Bill $naturalKey upserted")
    } yield ProcessingResult.Succeeded(naturalKey)
  }

  private def fetchCosponsorsFromDetail(
    detail: repcheck.shared.models.congress.dto.bill.BillDetailDTO,
    logCtx: LogContext,
  ): F[List[CoSponsorDTO]] = {
    val cosponsorUrl = detail.cosponsors.flatMap(_.url)
    cosponsorUrl match {
      case Some(url) =>
        logger.debug(logCtx, s"Fetching cosponsors from $url") *>
          apiClient.fetchCosponsors(url)
      case None =>
        Async[F].pure(List.empty[CoSponsorDTO])
    }
  }

  private def parseInstantStr(dateStr: String): Option[Instant] =
    scala.util.Try(Instant.parse(dateStr)).toOption

}
