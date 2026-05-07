package repcheck.ingestion.amendments.textcheck.app

import cats.effect.{Async, ExitCode}
import cats.syntax.all._

import fs2.Stream

import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}
import repcheck.pipeline.models.metadata.{ProcessingResult, StepRunSummary}

/**
 * Testable pipeline execution. Accepts a pre-built result stream + logger so tests can inject stubs without
 * constructing the full dependency graph. Mirrors the bill-side `PipelineExecutor` byte-for-byte — kept here so the
 * subproject doesn't have to depend on bill-text-availability-checker just for one helper.
 */
private[app] object PipelineExecutor {

  def execute[F[_]: Async](
    resultStream: Stream[F, ProcessingResult],
    logger: PipelineLogger[F],
    pipelineName: String,
    runId: Long,
  ): F[ExitCode] = {
    val logCtx = LogContext(runId = runId.toString, stepName = pipelineName)

    for {
      startedAt   <- Async[F].realTimeInstant
      results     <- executeStream(resultStream, logger, logCtx)
      completedAt <- Async[F].realTimeInstant
      summary = StepRunSummary.fromResults(
        stepRunId = 0L,
        stepName = pipelineName,
        startedAt = startedAt,
        completedAt = completedAt,
        results = results,
      )
      _ <- logSummary(logger, logCtx, summary)
      exitCode =
        if (summary.itemsFailed == 0) { ExitCode.Success }
        else { ExitCode.Error }
    } yield exitCode
  }

  private def executeStream[F[_]: Async](
    resultStream: Stream[F, ProcessingResult],
    logger: PipelineLogger[F],
    logCtx: LogContext,
  ): F[List[ProcessingResult]] =
    resultStream.compile.toList.handleErrorWith { error =>
      logger.error(logCtx, s"Stream failed: ${error.getMessage}", Some(error)) *>
        Async[F].raiseError(error)
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
