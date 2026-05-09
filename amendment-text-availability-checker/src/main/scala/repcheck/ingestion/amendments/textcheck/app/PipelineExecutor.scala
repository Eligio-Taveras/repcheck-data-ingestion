package repcheck.ingestion.amendments.textcheck.app

import cats.effect.{Async, ExitCode}
import cats.syntax.all._

import fs2.Stream

import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}
import repcheck.pipeline.models.metadata.{ProcessingResult, StepRunSummary}
import repcheck.pipeline.models.workflow.state.WorkflowStepStatus

/**
 * Testable pipeline execution. Accepts a pre-built result stream + logger so tests can inject stubs without
 * constructing the full dependency graph.
 *
 * The result stream is consumed via `compile.fold` over a small accumulator — no `compile.toList`, so memory stays
 * bounded regardless of how many amendments the cron tick scans. Each `ProcessingResult` flows in, increments the
 * appropriate counter, and is then released; the final [[StepRunSummary]] is constructed from the accumulated counts
 * once the stream completes.
 */
private[app] object PipelineExecutor {

  /**
   * @param runId
   *   workflow-run identifier from the IOApp's CLI args. Used as the LogContext `runId`. Currently a placeholder (`0L`
   *   default) until `workflow_runs` registration is wired up.
   * @param stepRunId
   *   workflow_run_steps row identifier from the IOApp's CLI args. Stored on the [[StepRunSummary]] as the foreign key
   *   into `workflow_run_steps`. Currently a placeholder (`0L` default) until that table is wired up.
   */
  def execute[F[_]: Async](
    resultStream: Stream[F, ProcessingResult],
    logger: PipelineLogger[F],
    pipelineName: String,
    runId: Long,
    stepRunId: Long = 0L,
  ): F[ExitCode] = {
    val logCtx = LogContext(runId = runId.toString, stepName = pipelineName)

    for {
      startedAt   <- Async[F].realTimeInstant
      stats       <- foldStream(resultStream, logger, logCtx)
      completedAt <- Async[F].realTimeInstant
      summary = buildSummary(stats, stepRunId, pipelineName, startedAt, completedAt)
      _ <- logSummary(logger, logCtx, summary)
      exitCode =
        if (summary.itemsFailed == 0) { ExitCode.Success }
        else { ExitCode.Error }
    } yield exitCode
  }

  /**
   * Stream-friendly counters. `errorCounts` keeps only the `(reason -> count)` map needed for the final summary — not
   * the full failed `ProcessingResult` payloads — so memory stays O(distinct-reason-strings) rather than O(total
   * amendments).
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
        // Skipped is a successful no-op (idempotent re-delivery, nothing-new-upstream); rolled into succeeded
        // so dashboards reflect "this run did its job" rather than treating a healthy skip as a separate bucket.
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

}
