package repcheck.ingestion.votes.app

import java.time.Instant

import cats.Monoid
import cats.effect.{Async, ExitCode}
import cats.syntax.all._

import fs2.Stream

import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}
import repcheck.pipeline.models.metadata.{ProcessingResult, StepRunSummary}
import repcheck.pipeline.models.workflow.state.WorkflowStepStatus

/**
 * Testable pipeline execution logic. Accepts a pre-built result stream and logger so tests can inject stubs without
 * needing to construct the full dependency graph.
 *
 * Aggregates results per-stream-step via a `Monoid[StepProgress]` and combines partial summaries with the `|+|`
 * operator, so the pipeline never materializes the full result list in memory. Each emitted `ProcessingResult`
 * contributes an incremental counter + error-count map; the fold is constant-memory regardless of result volume, and
 * the final `StepRunSummary` is assembled from the aggregated progress once the stream completes.
 */
private[app] object PipelineExecutor {

  /**
   * Per-stream-step progress accumulator. Contributed one-per-result by [[StepProgress.fromResult]] and combined via
   * the [[StepProgress.monoid]] instance. The fold's final value is materialized into a [[StepRunSummary]] at the end.
   */
  final private case class StepProgress(
    itemsProcessed: Int,
    itemsSucceeded: Int,
    itemsFailed: Int,
    errorCounts: Map[String, Int],
  )

  private object StepProgress {

    val empty: StepProgress = StepProgress(0, 0, 0, Map.empty)

    def fromResult(result: ProcessingResult): StepProgress =
      result match {
        case _: ProcessingResult.Succeeded => StepProgress(1, 1, 0, Map.empty)
        case f: ProcessingResult.Failed    => StepProgress(1, 0, 1, Map(f.reason -> 1))
        case _: ProcessingResult.Skipped   => StepProgress(1, 0, 0, Map.empty)
      }

    implicit val monoid: Monoid[StepProgress] = new Monoid[StepProgress] {
      override def empty: StepProgress = StepProgress.empty

      override def combine(a: StepProgress, b: StepProgress): StepProgress =
        StepProgress(
          itemsProcessed = a.itemsProcessed + b.itemsProcessed,
          itemsSucceeded = a.itemsSucceeded + b.itemsSucceeded,
          itemsFailed = a.itemsFailed + b.itemsFailed,
          // cats `Monoid[Map[K, V]]` (when `V: Semigroup`) combines values per key — here sums Int occurrence counts.
          errorCounts = a.errorCounts |+| b.errorCounts,
        )
    }

  }

  /**
   * @param runId
   *   run-level identifier for this pipeline execution, sourced from the launcher (or equivalent entry point) via
   *   [[repcheck.ingestion.common.execution.PipelineBootstrap.extractRunId]]. Used for log search across the whole run;
   *   per-item correlation IDs are generated independently by downstream processors for per-vote log search.
   */
  def execute[F[_]: Async](
    resultStream: Stream[F, ProcessingResult],
    logger: PipelineLogger[F],
    pipelineName: String,
    runId: String,
  ): F[ExitCode] = {
    val logCtx = LogContext(runId = runId, stepName = pipelineName)

    for {
      startedAt <- Async[F].realTimeInstant
      progress <- resultStream
        .map(StepProgress.fromResult)
        .compile
        .foldMonoid
      completedAt <- Async[F].realTimeInstant
      summary = buildSummary(pipelineName, progress, startedAt, completedAt)
      _ <- logger.info(
        logCtx,
        s"Pipeline completed: ${summary.itemsProcessed} processed, " +
          s"${summary.itemsSucceeded} succeeded, ${summary.itemsFailed} failed",
      )
      exitCode =
        if (summary.itemsFailed == 0) { ExitCode.Success }
        else { ExitCode.Error }
    } yield exitCode
  }

  private def buildSummary(
    pipelineName: String,
    progress: StepProgress,
    startedAt: Instant,
    completedAt: Instant,
  ): StepRunSummary =
    StepRunSummary(
      // TODO: replace with the DB-assigned stepRunId from `workflow_run_steps` once the WorkflowStateUpdater plumbing
      // (ingestion-common §3.7) is wired into votes-pipeline.
      stepRunId = 0L,
      stepName = pipelineName,
      status = statusFor(progress),
      startedAt = startedAt,
      completedAt = completedAt,
      itemsProcessed = progress.itemsProcessed,
      itemsSucceeded = progress.itemsSucceeded,
      itemsFailed = progress.itemsFailed,
      errorCounts = progress.errorCounts,
    )

  private def statusFor(progress: StepProgress): WorkflowStepStatus =
    if (progress.itemsProcessed == 0) {
      WorkflowStepStatus.Completed
    } else if (progress.itemsFailed == progress.itemsProcessed) {
      WorkflowStepStatus.Failed
    } else if (progress.itemsFailed > 0) {
      WorkflowStepStatus.CompletedWithErrors
    } else {
      WorkflowStepStatus.Completed
    }

}
