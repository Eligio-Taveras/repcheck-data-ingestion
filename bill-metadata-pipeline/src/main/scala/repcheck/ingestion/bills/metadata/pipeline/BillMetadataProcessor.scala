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
    new BillPersister[F](billRepo, cosponsorRepo, subjectRepo, historyArchiver, xa, logger)

  def streamAll(runId: Long): Stream[F, ProcessingResult] = {
    val fromDateTime = Instant.now().minus(config.lookbackDays.toLong, ChronoUnit.DAYS)
    val params       = repcheck.ingestion.common.api.FetchParams(fromDateTime = Some(fromDateTime))
    val logCtx       = LogContext(runId.toString, stepName)

    Stream.eval(Async[F].realTime).flatMap { start =>
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
        // The DEBUG log below makes filtered bills inspectable when log level is bumped to DEBUG;
        // suppressed by default to avoid log spam during a backfill that drops tens of thousands.
        .evalTap { li =>
          config.minCongress match {
            case Some(min) if li.congress < min =>
              logger.debug(
                logCtx,
                s"Filtered by minCongress=${min.toString}: " +
                  s"${li.congress.toString}-${li.billType.toUpperCase}-${li.number} " +
                  s"updateDate=${li.updateDate.fold("none")(identity)}",
              )
            case _ => Async[F].unit
          }
        }
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
        // Periodic progress checkpoint — emit one INFO every 250 results (one page-worth) so an
        // operator watching a long backfill can answer "is the pipeline making progress and at
        // what rate?" without grepping through per-bill INFO logs. Without this, a slow page-fetch
        // is indistinguishable from a deadlock; we directly diagnosed a healthy run as "stuck" for
        // 5 minutes during this PR's testing because per-bill output was being filtered/buffered
        // and there was no rolling counter to fall back on.
        .zipWithIndex
        .evalTap {
          case (result, idx) =>
            val n = idx + 1L
            if (n % 250L == 0L) {
              Async[F].realTime.flatMap { now =>
                val elapsedSec = (now - start).toSeconds
                val rate       = if (elapsedSec > 0L) n.toDouble / elapsedSec.toDouble else 0.0
                logger.info(
                  logCtx,
                  s"Sweep progress: ${n.toString} processed (last=${result.entityId}), " +
                    s"${elapsedSec.toString}s elapsed, " +
                    f"$rate%.1f bills/s",
                )
              }
            } else { Async[F].unit }
        }
        .map { case (result, _) => result }
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
      _ <- logger.debug(
        logCtx,
        s"[$naturalKey] processListItem.start url=${listItem.url} listUpdateDate=${listItem.updateDate.fold("none")(identity)}",
      )
      _      <- logger.debug(logCtx, s"[$naturalKey] processListItem.findByBillId.start")
      stored <- TransactionRunner.run(xa)(billRepo.findByBillId(naturalKey))
      _ <- logger.debug(
        logCtx,
        s"[$naturalKey] processListItem.findByBillId.done found=${stored.isDefined.toString} " +
          s"storedBillId=${stored.fold("none")(b => b.billId.toString)} " +
          s"storedUpdateDate=${stored.flatMap(_.updateDate).fold("none")(_.toString)}",
      )
      result <- evaluateAndProcess(listItem, naturalKey, stored, correlationId, logCtx)
      _      <- logger.debug(logCtx, s"[$naturalKey] processListItem.done resultType=${resultTypeName(result)}")
    } yield result
  }

  private def resultTypeName(r: ProcessingResult): String = r match {
    case _: ProcessingResult.Succeeded => "Succeeded"
    case _: ProcessingResult.Skipped   => "Skipped"
    case _: ProcessingResult.Failed    => "Failed"
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

    for {
      _ <- logger.debug(
        logCtx,
        s"[$naturalKey] evaluateAndProcess incomingDate=${incomingDate.fold("none")(_.toString)} " +
          s"storedDate=${storedDate.fold("none")(_.toString)} " +
          s"isNew=${stored.isEmpty.toString}",
      )
      result <- stored match {
        case None =>
          logger.info(logCtx, s"New bill detected: $naturalKey") *>
            processBill(listItem, naturalKey, isNew = true, stored, correlationId, logCtx)

        // Placeholder detection — ignore the updateDate comparison for stub rows.
        //
        // Other pipelines (bill-text-availability-checker, bill-summary-pipeline) can insert a
        // placeholder bill row when they need to reference a bill that the metadata pipeline
        // hasn't ingested yet. The placeholder has natural-key fields populated, `title` empty,
        // every detail Option = None, and `update_date` set to its insertion timestamp. That
        // insertion time is by definition newer than the API's real updateDate for any bill
        // that hasn't been modified recently — which means the date-comparison guard below
        // would mark the row "Bill unchanged" and the placeholder would never be backfilled.
        // Routing empty-title rows through the update path forces a detail fetch that fills
        // title, sponsor, dates, and the rest. Empty title is the canonical placeholder marker:
        // legitimately-ingested bills always have a non-empty title (Congress.gov populates it
        // from the bill's official title even when other fields are sparse). 161,721 such rows
        // existed in the local dev DB at the time this branch was added, ~67% of all bills, all
        // stuck on the unchanged path until this fix.
        case Some(s) if s.title.isEmpty =>
          logger.info(logCtx, s"Placeholder bill detected, backfilling: $naturalKey") *>
            processBill(listItem, naturalKey, isNew = false, stored, correlationId, logCtx)

        case Some(_) if incomingDate.exists(inc => storedDate.forall(sd => inc.isAfter(sd))) =>
          logger.info(
            logCtx,
            s"Updated bill detected: $naturalKey (incoming=${incomingDate
                .fold("none")(_.toString)} > stored=${storedDate.fold("none")(_.toString)})",
          ) *>
            processBill(listItem, naturalKey, isNew = false, stored, correlationId, logCtx)

        case Some(_) =>
          logger.info(logCtx, s"Bill unchanged: $naturalKey") *>
            Async[F].pure[ProcessingResult](ProcessingResult.Skipped(naturalKey, "unchanged"))
      }
    } yield result
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
    // Step-by-step INFO logs through the per-bill processing path. If a bill ever stalls in this
    // method (codec hang, slow upstream, DB lock, etc.) the LAST log line tells the operator the
    // exact step that didn't return — turning every "what's the pipeline doing right now?"
    // diagnosis from grep-the-haystack into "look at the most recent line for this correlationId".
    for {
      _      <- logger.info(ctx, s"[$naturalKey] step=fetchDetail.start url=${listItem.url}")
      detail <- apiClient.fetchDetail(listItem.url)
      _      <- logger.info(ctx, s"[$naturalKey] step=fetchDetail.done")
      conversionResult <- Async[F].fromEither(
        detail.toDO.leftMap(reason => BillProcessingFailed(naturalKey, s"DTO-to-DO conversion failed: $reason"))
      )
      _ <- logger.info(ctx, s"[$naturalKey] step=convertDTO.done subjects=${conversionResult.subjects.size.toString}")
      cosponsorDTOs <- fetchCosponsorsFromDetail(detail, ctx)
      _ <- logger.info(
        ctx,
        s"[$naturalKey] step=fetchCosponsors.done cosponsorCount=${cosponsorDTOs.size.toString}",
      )
      billDO <- memberResolver.ensureSponsorPlaceholder(conversionResult.bill, detail, ctx)
      _ <- logger.info(
        ctx,
        s"[$naturalKey] step=resolveSponsor.done sponsorMemberId=${billDO.sponsorMemberId.fold("none")(_.toString)}",
      )
      cosponsorDOs <- memberResolver.buildCosponsorDOs(cosponsorDTOs, ctx)
      _ <- logger.info(
        ctx,
        s"[$naturalKey] step=resolveCosponsors.done resolved=${cosponsorDOs.size.toString}/${cosponsorDTOs.size.toString}",
      )
      _ <- logger.info(ctx, s"[$naturalKey] step=persist.start isNew=${isNew.toString}")
      _ <- billPersister.persistBill(billDO, conversionResult.subjects, cosponsorDOs, naturalKey, isNew)
      _ <- logger.info(ctx, s"Bill $naturalKey upserted")
    } yield ProcessingResult.Succeeded(naturalKey)
  }

  private def fetchCosponsorsFromDetail(
    detail: repcheck.shared.models.congress.dto.bill.BillDetailDTO,
    logCtx: LogContext,
  ): F[List[CoSponsorDTO]] = {
    val cosponsorUrl = detail.cosponsors.flatMap(_.url)
    cosponsorUrl match {
      case Some(url) =>
        for {
          _      <- logger.debug(logCtx, s"fetchCosponsorsFromDetail.url=$url")
          result <- apiClient.fetchCosponsors(url)
          _      <- logger.debug(logCtx, s"fetchCosponsorsFromDetail.done count=${result.size.toString}")
        } yield result
      case None =>
        logger.debug(logCtx, s"fetchCosponsorsFromDetail.skip (no cosponsors URL on detail)") *>
          Async[F].pure(List.empty[CoSponsorDTO])
    }
  }

  private def parseInstantStr(dateStr: String): Option[Instant] =
    scala.util.Try(Instant.parse(dateStr)).toOption

}
