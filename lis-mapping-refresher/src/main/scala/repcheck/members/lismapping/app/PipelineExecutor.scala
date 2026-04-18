package repcheck.members.lismapping.app

import java.util.UUID

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

  def execute[F[_]: Async](
    resultStream: Stream[F, ProcessingResult],
    logger: PipelineLogger[F],
    pipelineName: String,
    correlationId: UUID,
  ): F[ExitCode] = {
    val logCtx = LogContext(runId = correlationId.toString, stepName = pipelineName)

    for {
      startedAt   <- Async[F].realTimeInstant
      results     <- resultStream.compile.toList
      completedAt <- Async[F].realTimeInstant
      summary = StepRunSummary.fromResults(
        stepRunId = 0L,
        stepName = pipelineName,
        startedAt = startedAt,
        completedAt = completedAt,
        results = results,
      )
      _ <- logger.info(
        logCtx,
        s"Pipeline completed: ${summary.itemsProcessed.toString} processed, ${summary.itemsSucceeded.toString} succeeded, ${summary.itemsFailed.toString} failed",
      )
      exitCode =
        if (summary.itemsFailed == 0) { ExitCode.Success }
        else { ExitCode.Error }
    } yield exitCode
  }

}
