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
import repcheck.ingestion.bills.summary.errors.BillSummariesApiErrorClassifier
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
class BillSummaryProcessor[F[_]: Async](
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
   *
   * @param runId
   *   run-level identifier sourced by the launcher (`workflow_runs.id` string). Threaded into every log line via
   *   [[LogContext.runId]] so cross-pipeline tracing keys on the same value the launcher created.
   */
  def streamAll(runId: String): Stream[F, ProcessingResult] = {
    val logCtx = LogContext(runId = runId, stepName = config.stepName)

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
          .flatMap { congress =>
            // Isolate per-congress failures: if a sustained network/API error in one congress
            // exhausts retries, we surface it as a single ProcessingResult.Failed and continue
            // with the next congress instead of killing the whole multi-congress run. Per-bill
            // failures are already isolated inside `processOneBill.handleErrorWith`; this layer
            // catches errors that escape the per-bill boundary (e.g. fetch failures from
            // `apiClient.fetchAll` or DB errors at the chunk level).
            streamForCongress(congress, from, to, logCtx).handleErrorWith { error =>
              Stream.eval(
                logger
                  .error(
                    logCtx,
                    s"Congress $congress failed at the stream level (continuing with remaining congresses): ${error.getMessage}",
                    Some(error),
                  )
                  .as(
                    ProcessingResult
                      .Failed(s"congress-$congress", error.getMessage, classifyError(error))
                  )
              )
            }
          }
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
    mapped.groupBy(_._1.naturalKey).flatMap {
      case (naturalKey, entries) =>
        maxByProgressionOrder(entries).map(naturalKey -> _)
    }
  }

  /**
   * `Wart.IterableOps` blocks `Iterable.maxBy` because it throws on empty collections. Implemented as a `foldLeft`
   * starting from the explicit `head` of a non-empty pattern match — returns `None` for empty inputs (shouldn't happen
   * coming from `groupBy.values` but the compiler doesn't know that).
   */
  private[pipeline] def maxByProgressionOrder(
    entries: List[(BillReferenceDTO, BillSummaryDTO, TextVersionCode)]
  ): Option[(BillReferenceDTO, BillSummaryDTO, TextVersionCode)] =
    entries match {
      case Nil => None
      case head :: tail =>
        Some(tail.foldLeft(head) { (best, cur) =>
          if (cur._3.progressionOrder > best._3.progressionOrder) cur else best
        })
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
  ): F[ProcessingResult] = {
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
            logger.debug(logCtx, s"Updated expected_text_version_code for $naturalKey to $newStage").as(result)
          case ProcessingResult.Skipped(_, reason) =>
            logger.debug(logCtx, s"Skipped $naturalKey: $reason").as(result)
          case ProcessingResult.Failed(_, _, _) =>
            // We don't construct Failed results in `txn`; this branch is here only for exhaustiveness.
            Async[F].pure(result)
        }
      }
      .handleErrorWith { error =>
        // Write attempt failed (unexpected DB error, transient connection issue, etc.). Surface as Failed so the
        // pipeline counters reflect it; the run continues with subsequent bills.
        logger
          .error(logCtx, s"Failed to process summary for $naturalKey: ${error.getMessage}", Some(error))
          .as(ProcessingResult.Failed(naturalKey, error.getMessage, classifyError(error)))
      }
  }

  /**
   * Map `Throwable` → ErrorClass label string for `ProcessingResult.Failed`. Defers to
   * [[BillSummariesApiErrorClassifier]] for HTTP + network-level transient detection (it walks the cause chain), with
   * two locally-relevant exceptions handled here: unrecognized summary version codes are `Systemic` (fail-fast — the
   * operator must add the missing entry to the catalog) and `SQLTransientException` is `Transient`.
   */
  private[pipeline] def classifyError(error: Throwable): String =
    error match {
      case _: UnrecognizedSummaryVersionCode => "Systemic"
      case _: java.sql.SQLTransientException => "Transient"
      case other                             => BillSummariesApiErrorClassifier.classify(other).toString
    }

}
