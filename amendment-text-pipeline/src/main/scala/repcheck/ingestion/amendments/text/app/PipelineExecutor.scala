package repcheck.ingestion.amendments.text.app

import cats.effect.{Async, ExitCode}
import cats.syntax.all._

import fs2.Stream

import repcheck.ingestion.common.execution.WorkflowStateUpdater
import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}
import repcheck.pipeline.models.metadata.{ProcessingResult, StepRunSummary}
import repcheck.pipeline.models.workflow.state.WorkflowStepStatus

/**
 * Testable pipeline execution logic. Mirror of the bill-side [[repcheck.ingestion.bills.text.app.PipelineExecutor]] —
 * accepts a pre-built result stream and logger so tests can inject stubs without constructing the full dependency
 * graph. When a `WorkflowStateUpdater` is provided, records step start/completion/failure in `workflow_run_steps` for
 * observability.
 *
 * The result stream is consumed via `compile.fold` over a small accumulator — no `compile.toList`, so memory stays
 * bounded regardless of how many events the run processes. Each `ProcessingResult` flows in, increments the appropriate
 * counter, and is then released; the final [[StepRunSummary]] is constructed from the accumulated counts once the
 * stream completes. Same shape as the §7.5 availability-checker's executor.
 */
private[app] object PipelineExecutor {

  /**
   * @param runId
   *   workflow-run identifier from the IOApp's CLI args. Used as the LogContext `runId` and the `WorkflowStateUpdater`
   *   key. Defaults to `0L` — placeholder used when no workflow registrar is in scope.
   * @param stepRunId
   *   workflow_run_steps row identifier from the IOApp's CLI args. Stored on the [[StepRunSummary]] as the foreign key
   *   into `workflow_run_steps`. Defaults to `0L` — placeholder until that table is wired up.
   */
  def execute[F[_]: Async](
    resultStream: Stream[F, ProcessingResult],
    logger: PipelineLogger[F],
    pipelineName: String,
    runId: Long,
    stepRunId: Long = 0L,
    workflowStateUpdater: Option[WorkflowStateUpdater[F]] = None,
  ): F[ExitCode] = {
    val logCtx = LogContext(runId = runId.toString, stepName = pipelineName)

    for {
      _           <- recordStepStarted(workflowStateUpdater, runId, pipelineName)
      startedAt   <- Async[F].realTimeInstant
      stats       <- foldStream(resultStream, logger, logCtx)
      completedAt <- Async[F].realTimeInstant
      summary = buildSummary(stats, stepRunId, pipelineName, startedAt, completedAt)
      _ <- logSummary(logger, logCtx, summary)
      exitCode =
        if (summary.itemsFailed == 0) { ExitCode.Success }
        else { ExitCode.Error }
      _ <- recordStepOutcome(workflowStateUpdater, runId, pipelineName, summary)
    } yield exitCode
  }

  /**
   * Stream-friendly counters. `errorCounts` keeps only the `(reason -> count)` map needed for the final summary — not
   * the full failed `ProcessingResult` payloads — so memory stays O(distinct-reason-strings) rather than O(total
   * events).
   */
  final private[app] case class StreamingStats(
    itemsProcessed: Int,
    itemsSucceeded: Int,
    itemsFailed: Int,
    errorCounts: Map[String, Int],
  )

  private[app] object StreamingStats {
    val empty: StreamingStats = StreamingStats(0, 0, 0, Map.empty)
  }

  private[app] def addResult(stats: StreamingStats, result: ProcessingResult): StreamingStats =
    result match {
      case _: ProcessingResult.Succeeded =>
        stats.copy(
          itemsProcessed = stats.itemsProcessed + 1,
          itemsSucceeded = stats.itemsSucceeded + 1,
        )
      case _: ProcessingResult.Skipped =>
        // Skipped is a successful no-op (idempotent re-delivery, already-ingested); rolled into succeeded so
        // dashboards reflect "this run did its job" rather than treating a healthy skip as a separate bucket.
        stats.copy(
          itemsProcessed = stats.itemsProcessed + 1,
          itemsSucceeded = stats.itemsSucceeded + 1,
        )
      case f: ProcessingResult.Failed =>
        val updatedCounts = stats.errorCounts.updatedWith(f.reason) {
          case Some(n) => Some(n + 1)
          case None    => Some(1)
        }
        stats.copy(
          itemsProcessed = stats.itemsProcessed + 1,
          itemsFailed = stats.itemsFailed + 1,
          errorCounts = updatedCounts,
        )
    }

  private def foldStream[F[_]: Async](
    resultStream: Stream[F, ProcessingResult],
    logger: PipelineLogger[F],
    logCtx: LogContext,
  ): F[StreamingStats] =
    resultStream.compile.fold(StreamingStats.empty)(addResult).handleErrorWith { error =>
      logger.error(logCtx, s"Stream failed: ${error.getMessage}", Some(error)) *>
        Async[F].raiseError(error)
    }

  private[app] def buildSummary(
    stats: StreamingStats,
    stepRunId: Long,
    pipelineName: String,
    startedAt: java.time.Instant,
    completedAt: java.time.Instant,
  ): StepRunSummary = {
    val status: WorkflowStepStatus =
      if (stats.itemsProcessed == 0) {
        WorkflowStepStatus.Completed
      } else if (stats.itemsFailed == stats.itemsProcessed) {
        WorkflowStepStatus.Failed
      } else if (stats.itemsFailed > 0) {
        WorkflowStepStatus.CompletedWithErrors
      } else {
        WorkflowStepStatus.Completed
      }

    StepRunSummary(
      stepRunId = stepRunId,
      stepName = pipelineName,
      status = status,
      startedAt = startedAt,
      completedAt = completedAt,
      itemsProcessed = stats.itemsProcessed,
      itemsSucceeded = stats.itemsSucceeded,
      itemsFailed = stats.itemsFailed,
      errorCounts = stats.errorCounts,
    )
  }

  private def logSummary[F[_]](
    logger: PipelineLogger[F],
    logCtx: LogContext,
    summary: StepRunSummary,
  ): F[Unit] =
    logger.info(
      logCtx,
      s"Pipeline completed: ${summary.itemsProcessed.toString} processed, " +
        s"${summary.itemsSucceeded.toString} succeeded, ${summary.itemsFailed.toString} failed",
    )

  private def recordStepStarted[F[_]: Async](
    updater: Option[WorkflowStateUpdater[F]],
    runId: Long,
    stepName: String,
  ): F[Unit] =
    updater.traverse_(_.recordStepStarted(runId.toString, stepName))

  private def recordStepOutcome[F[_]: Async](
    updater: Option[WorkflowStateUpdater[F]],
    runId: Long,
    stepName: String,
    summary: StepRunSummary,
  ): F[Unit] =
    updater.traverse_ { wsu =>
      if (summary.itemsFailed == 0) {
        wsu.recordStepCompleted(runId.toString, stepName)
      } else {
        wsu.recordStepFailed(
          runId.toString,
          stepName,
          s"${summary.itemsFailed} of ${summary.itemsProcessed} items failed",
        )
      }
    }

}
