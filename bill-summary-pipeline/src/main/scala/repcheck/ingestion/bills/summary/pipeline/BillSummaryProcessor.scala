package repcheck.ingestion.bills.summary.pipeline

import java.time.Instant
import java.time.temporal.ChronoUnit

import cats.effect.Async
import cats.syntax.all._

import fs2.Stream

import doobie._

import repcheck.ingestion.bills.common.persistence.{BillRepository, TransactionRunner}
import repcheck.ingestion.bills.summary.api.BillSummariesApiClient
import repcheck.ingestion.bills.summary.config.BillSummaryConfig
import repcheck.ingestion.bills.summary.persistence.WorkflowRunStepsRepository
import repcheck.ingestion.common.api.FetchParams
import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}
import repcheck.pipeline.models.metadata.ProcessingResult
import repcheck.shared.models.congress.bill.{SummaryVersionCodeMapper, TextVersionCode, UnrecognizedSummaryVersionCode}
import repcheck.shared.models.congress.dto.bill.{BillReferenceDTO, BillSummaryDTO}

/**
 * Processes the Congress.gov `/summaries[/{congress}]` feed end-to-end:
 *   1. Read the watermark from `workflow_run_steps` (last successful end time for `config.stepName`); fall back to
 *      `config.initialLookbackDays` if no prior run. 2. Iterate `config.congresses` sequentially; for each, paginate
 *      `/summaries/{congress}?fromDateTime=&toDateTime=`. 3. For each page, group entries by bill natural key and pick
 *      the highest-`progressionOrder` summary per bill — avoids redundant DB hits when a bill has multiple summaries in
 *      the window. 4. For each (bill, summary) pair: ensure the bill row exists (`upsertPlaceholder` if missing), then
 *      conditionally `updateExpectedVersion` — only if the new stage's `progressionOrder` exceeds the current stored
 *      stage's. This is the same regression guard `bill-metadata-pipeline.BillPersister.applyExpectedVersionFloor`
 *      uses, so neither writer can downgrade the other.
 *
 * Yields one [[ProcessingResult]] per processed bill so [[PipelineExecutor]] can roll up `StepRunSummary` counters.
 * Unknown summary versionCodes raise [[UnrecognizedSummaryVersionCode]] (Systemic) and short-circuit the entire run —
 * fail-fast posture so the operator adds the missing entry to the catalog and redeploys.
 */
class BillSummaryProcessor[F[_]: Async] private[pipeline] (
  apiClient: BillSummariesApiClient[F],
  billRepo: BillRepository[ConnectionIO],
  workflowRepo: WorkflowRunStepsRepository[ConnectionIO],
  xa: Transactor[F],
  config: BillSummaryConfig,
  logger: PipelineLogger[F],
) {

  /**
   * Build the result stream for one pipeline run. The stream lazily computes the watermark, then folds across the
   * configured congresses, emitting one `ProcessingResult` per bill for which we attempted a write.
   */
  def streamAll(runId: Long): Stream[F, ProcessingResult] = {
    val logCtx = LogContext(runId = runId.toString, stepName = config.stepName)

    val watermarkProgram: F[(Instant, Instant)] = for {
      now            <- Async[F].realTimeInstant
      lastSuccessOpt <- TransactionRunner.run(xa)(workflowRepo.lastSuccessfulEndAt(config.stepName))
      bufferSeconds = config.watermarkBuffer.toSeconds
      from = lastSuccessOpt
        .map(_.minusSeconds(bufferSeconds))
        .getOrElse(now.minus(config.initialLookbackDays.toLong, ChronoUnit.DAYS))
      _ <- logger.info(
        logCtx,
        s"Computed watermark: fromDateTime=$from toDateTime=$now (source=${lastSuccessOpt.fold("initial-lookback")(_ => "workflow_run_steps")})",
      )
    } yield (from, now)

    Stream.eval(watermarkProgram).flatMap {
      case (from, to) =>
        Stream
          .emits(config.congresses)
          .flatMap(congress => streamForCongress(congress, from, to, logCtx))
    }
  }

  /**
   * Paginate `/summaries/{congress}` across the watermark window, group each page's entries by bill (taking the
   * highest-stage summary per bill within that page), and write each through the cooperative-update path.
   */
  private[pipeline] def streamForCongress(
    congress: Int,
    fromDateTime: Instant,
    toDateTime: Instant,
    logCtx: LogContext,
  ): Stream[F, ProcessingResult] = {
    val params = FetchParams(
      congress = Some(congress),
      fromDateTime = Some(fromDateTime),
      toDateTime = Some(toDateTime),
    )

    apiClient
      .fetchAll(params)
      .chunks
      .flatMap { chunk =>
        val byNaturalKey = pickHighestPerBill(chunk.toList)
        Stream.evalSeq(byNaturalKey.toList.traverse {
          case (naturalKey, summary) => processOneBill(naturalKey, summary, logCtx)
        })
      }
  }

  /**
   * Within a single chunk of summaries, group by `bill.naturalKey` and keep only the entry whose mapped
   * [[TextVersionCode]] has the highest `progressionOrder`. Entries without a `bill` reference (the bill-scoped
   * `/bill/{c}/{t}/{n}/summaries` shape — should not appear from the global endpoint but we tolerate it) are skipped.
   * Entries whose `versionCode` is `None` are skipped silently — malformed API response, not a fail-fast case.
   */
  private[pipeline] def pickHighestPerBill(
    summaries: List[BillSummaryDTO]
  ): Map[String, (BillReferenceDTO, BillSummaryDTO, TextVersionCode)] = {
    val mapped: List[(BillReferenceDTO, BillSummaryDTO, TextVersionCode)] = summaries.flatMap { s =>
      for {
        billRef <- s.bill
        code    <- s.versionCode
        mapped  <- SummaryVersionCodeMapper.toTextVersionCode(code).toOption
      } yield (billRef, s, mapped)
    }
    mapped.groupBy(_._1.naturalKey).map {
      case (naturalKey, entries) =>
        val winner = entries.maxBy(_._3.progressionOrder)
        (naturalKey, winner)
    }
  }

  /**
   * Process one bill's highest-stage summary in this chunk: ensure the bill exists (placeholder if missing), read the
   * current expected stage, write the new stage if it advances. All three operations run in a single `ConnectionIO`
   * transaction so a partial failure doesn't leave a placeholder without an `expected_text_version_code`.
   */
  private[pipeline] def processOneBill(
    naturalKey: String,
    triple: (BillReferenceDTO, BillSummaryDTO, TextVersionCode),
    logCtx: LogContext,
  ): F[List[ProcessingResult]] = {
    val (_, _, newStage) = triple
    val txn: ConnectionIO[ProcessingResult] = for {
      _        <- billRepo.upsertPlaceholder(naturalKey)
      existing <- billRepo.findExpectedVersion(naturalKey)
      shouldWrite = existing match {
        case None          => true
        case Some(current) => newStage.progressionOrder > current.progressionOrder
      }
      result <-
        if (shouldWrite) {
          billRepo
            .updateExpectedVersion(naturalKey, newStage)
            .as(ProcessingResult.Succeeded(naturalKey, eventEmitted = false))
        } else {
          doobie.free.connection.pure(
            ProcessingResult.Skipped(naturalKey, s"already-at-or-past-stage:$newStage")
          )
        }
    } yield result

    TransactionRunner
      .run(xa)(txn)
      .flatMap { result =>
        result match {
          case ProcessingResult.Succeeded(_, _) =>
            logger.debug(logCtx, s"Updated expected_text_version_code for $naturalKey to $newStage").as(List(result))
          case ProcessingResult.Skipped(_, reason) =>
            logger.debug(logCtx, s"Skipped $naturalKey: $reason").as(List(result))
          case ProcessingResult.Failed(_, _, _) =>
            // We don't construct Failed results in `txn`; this branch is here only for exhaustiveness.
            Async[F].pure(List(result))
        }
      }
      .handleErrorWith { error =>
        // Write attempt failed (unexpected DB error, transient connection issue, etc.). Surface as Failed so the
        // pipeline counters reflect it; the run continues with subsequent bills.
        logger
          .error(logCtx, s"Failed to process summary for $naturalKey: ${error.getMessage}", Some(error))
          .as(List(ProcessingResult.Failed(naturalKey, error.getMessage, classifyError(error))))
      }
  }

  private[pipeline] def classifyError(error: Throwable): String =
    error match {
      case _: UnrecognizedSummaryVersionCode  => "Systemic"
      case _: java.net.SocketTimeoutException => "Transient"
      case _: java.net.ConnectException       => "Transient"
      case _: java.io.IOException             => "Transient"
      case _: java.sql.SQLTransientException  => "Transient"
      case _                                  => "Systemic"
    }

}
