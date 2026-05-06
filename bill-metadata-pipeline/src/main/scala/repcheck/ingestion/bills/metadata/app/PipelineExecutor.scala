package repcheck.ingestion.bills.metadata.app

import cats.effect.{Async, ExitCode}
import cats.syntax.all._

import fs2.Stream

import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}
import repcheck.pipeline.models.metadata.{ProcessingResult, StepRunSummary}

/**
 * Testable pipeline execution logic. Accepts a pre-built result stream and logger so that tests can inject stubs
 * without needing to construct the full dependency graph.
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
      _ <- logger.info(logCtx, s"PipelineExecutor.execute.start pipelineName=$pipelineName runId=${runId.toString}")
      startedAt   <- Async[F].realTimeInstant
      _           <- logger.info(logCtx, s"PipelineExecutor.compile.start startedAt=${startedAt.toString}")
      results     <- resultStream.compile.toList
      completedAt <- Async[F].realTimeInstant
      _ <- logger.info(
        logCtx,
        s"PipelineExecutor.compile.done completedAt=${completedAt.toString} " +
          s"resultsCount=${results.size.toString} " +
          s"durationMs=${(completedAt.toEpochMilli - startedAt.toEpochMilli).toString}",
      )
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
      _ <- logger.info(logCtx, s"PipelineExecutor.execute.exit exitCode=${exitCode.toString}")
    } yield exitCode
  }

}
