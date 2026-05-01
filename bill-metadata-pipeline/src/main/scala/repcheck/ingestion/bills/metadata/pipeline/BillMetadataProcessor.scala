package repcheck.ingestion.bills.metadata.pipeline

import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.UUID

import cats.effect.Async
import cats.syntax.all._

import fs2.Stream

import doobie._

import repcheck.ingestion.bills.common.persistence.{
  BillCosponsorRepository,
  BillHistoryArchiver,
  BillRepository,
  BillSubjectRepository,
  TransactionRunner,
}
import repcheck.ingestion.bills.metadata.api.BillsApiClient
import repcheck.ingestion.bills.metadata.config.BillMetadataConfig
import repcheck.ingestion.bills.metadata.errors.BillProcessingFailed
import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}
import repcheck.ingestion.common.placeholders.{EntityRepository, PlaceholderCreator}
import repcheck.members.common.persistence.MemberRepository
import repcheck.pipeline.models.metadata.ProcessingResult
import repcheck.shared.models.congress.dos.bill.BillDO
import repcheck.shared.models.congress.dos.member.MemberDO
import repcheck.shared.models.congress.dto.bill.{BillListItemDTO, CoSponsorDTO}
import repcheck.shared.models.congress.dto.conversions.BillConversions._

class BillMetadataProcessor[F[_]: Async](
  apiClient: BillsApiClient[F],
  billRepo: BillRepository[ConnectionIO],
  cosponsorRepo: BillCosponsorRepository[ConnectionIO],
  subjectRepo: BillSubjectRepository[ConnectionIO],
  historyArchiver: BillHistoryArchiver[ConnectionIO],
  memberRepo: MemberRepository,
  placeholderCreator: PlaceholderCreator[F],
  memberEntityRepo: EntityRepository[F, MemberDO],
  xa: Transactor[F],
  config: BillMetadataConfig,
  logger: PipelineLogger[F],
) {

  private val stepName = "bill-metadata"

  private val memberResolver =
    new MemberResolver[F](memberRepo, placeholderCreator, memberEntityRepo, xa, logger)

  private val billPersister =
    new BillPersister[F](billRepo, cosponsorRepo, subjectRepo, historyArchiver, xa)

  def streamAll(runId: Long): Stream[F, ProcessingResult] = {
    val fromDateTime = Instant.now().minus(config.lookbackDays.toLong, ChronoUnit.DAYS)
    val params       = repcheck.ingestion.common.api.FetchParams(fromDateTime = Some(fromDateTime))
    val logCtx       = LogContext(runId.toString, stepName)

    Stream.eval(
      logger.info(
        logCtx,
        s"Starting metadata sweep (lookbackDays=${config.lookbackDays.toString}, " +
          s"parallelism=${config.parallelism.toString}, fromDateTime=${fromDateTime.toString}, " +
          s"minCongress=${config.minCongress.fold("none")(_.toString)})",
      )
    ) *> apiClient
      .fetchAll(params)
      .handleErrorWith { e =>
        // Page-level fetch failures used to be silently swallowed here (the stream returned
        // `Stream.empty` and the pipeline reported "completing with partial results" + exit code 0).
        // That made every transient connection drop look like a successful run, which masked a
        // real backfill abandonment for hours of expected work — most recent observation: a 13-year
        // lookback crashed at offset=4250 (page 18 of ~hundreds), pipeline reported
        // "4250 succeeded, 0 failed" and exited 0 despite leaving every older bill unenriched.
        //
        // Now we log + re-raise so the failure propagates to the IOApp `run` method's exit code,
        // making the run visibly fail. Per-bill failures (below) are still recovered to
        // `ProcessingResult.Failed` so a single bad row doesn't kill the whole stream — that's
        // the right per-item posture. Page-level failures are different: they truncate the entire
        // remaining backfill and must surface.
        Stream.eval(
          logger.error(logCtx, s"Page fetch failed, aborting run: ${e.getMessage}", Some(e))
        ) *> Stream.raiseError[F](e)
      }
      // Drop bills from congresses below the configured floor before they reach the parallel stage.
      // A 30-day production sweep can surface 19th-century bills when Congress.gov republishes them
      // with fresh updateDate values; their detail-level schema (object-vs-array `bill` form, missing
      // required fields, fractional bill numbers like `1025½` that fail PG INTEGER inserts) cannot
      // be deserialized reliably. Filtering at the list-item level — where `congress` is a stable
      // Int — avoids the wasted detail fetch + retry budget and keeps per-item Failed counts clean.
      .filter(li => config.minCongress.forall(min => li.congress >= min))
      .parEvalMap(config.parallelism) { listItem =>
        val naturalKey    = buildNaturalKey(listItem)
        val correlationId = UUID.randomUUID()
        val logCtxItem    = LogContext(runId.toString, stepName, Some(correlationId), Some(naturalKey))
        processListItem(listItem, correlationId).handleErrorWith { e =>
          logger.error(logCtxItem, s"Failed to process $naturalKey: ${e.getMessage}", Some(e)) *>
            Async[F].pure(ProcessingResult.Failed(naturalKey, e.getMessage))
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
    val storedDate   = stored.flatMap(_.updateDate)

    stored match {
      case None =>
        logger.info(logCtx, s"New bill detected: $naturalKey") *>
          processBill(listItem, naturalKey, isNew = true, stored, correlationId, logCtx)

      case Some(_) if incomingDate.exists(inc => storedDate.forall(sd => inc.isAfter(sd))) =>
        logger.info(logCtx, s"Updated bill detected: $naturalKey") *>
          processBill(listItem, naturalKey, isNew = false, stored, correlationId, logCtx)

      case Some(_) =>
        // INFO so the skip path is visible during a sweep — without it, an operator can't tell
        // whether the pipeline is processing slowly or churning through unchanged-bill no-ops.
        // Mirrors the diagnostic-logs treatment in the bill-text-availability-checker (PR #99).
        logger.info(logCtx, s"Bill unchanged: $naturalKey") *>
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
