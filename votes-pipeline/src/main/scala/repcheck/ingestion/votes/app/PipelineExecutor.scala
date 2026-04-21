package repcheck.ingestion.votes.app

import cats.effect.{Async, ExitCode}
import cats.syntax.all._

import fs2.Stream

import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}
import repcheck.pipeline.models.metadata.{ProcessingResult, StepRunSummary}

/**
 * Testable pipeline execution logic. Accepts a pre-built result stream and logger so tests can inject stubs without
 * needing to construct the full dependency graph.
 *
 * Duplicated from `bill-metadata-pipeline`'s equivalent — the logic is pipeline-agnostic. A follow-up could promote
 * this to a shared location (e.g., ingestion-common) if a third pipeline picks it up; for now the local copy matches
 * the existing per-pipeline convention in the repo.
 */
private[app] object PipelineExecutor {

  /**
   * @param runId
   *   run-level identifier for this pipeline execution. Will be the Long ID from the `workflow_runs` DB row once
   *   `PipelineBootstrap.extractRunId` (ingestion-common §3.7) is implemented; currently a placeholder.
   */
  def execute[F[_]: Async](
    resultStream: Stream[F, ProcessingResult],
    logger: PipelineLogger[F],
    pipelineName: String,
    runId: Long,
  ): F[ExitCode] = {
    val logCtx = LogContext(runId = runId.toString, stepName = pipelineName)

    for {
      startedAt   <- Async[F].realTimeInstant
      results     <- resultStream.compile.toList
      completedAt <- Async[F].realTimeInstant
      // TODO: record step started/completed in workflow_run_steps via WorkflowStateUpdater (ingestion-common §3.7).
      // stepRunId is a placeholder until that table and updater are implemented.
      summary = StepRunSummary.fromResults(
        stepRunId = 0L,
        stepName = pipelineName,
        startedAt = startedAt,
        completedAt = completedAt,
        results = results,
      )
      _ <- logger.info(
        logCtx,
        s"Pipeline completed: ${summary.itemsProcessed} processed, ${summary.itemsSucceeded} succeeded, ${summary.itemsFailed} failed",
      )
      exitCode =
        if (summary.itemsFailed == 0) { ExitCode.Success }
        else { ExitCode.Error }
    } yield exitCode
  }

}
