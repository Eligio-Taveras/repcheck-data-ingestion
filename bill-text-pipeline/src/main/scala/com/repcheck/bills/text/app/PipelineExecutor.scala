package com.repcheck.bills.text.app

import java.util.UUID

import cats.effect.{Async, ExitCode}
import cats.syntax.all._

import fs2.Stream

import repcheck.ingestion.common.execution.WorkflowStateUpdater
import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}
import repcheck.pipeline.models.metadata.{ProcessingResult, StepRunSummary}

/**
 * Testable pipeline execution logic. Accepts a pre-built result stream and logger so that tests can inject stubs
 * without needing to construct the full dependency graph.
 *
 * When a `WorkflowStateUpdater` is provided, records step start/completion/failure in the `workflow_run_steps` table
 * for observability. The updater is optional so that unit tests can run without a database.
 */
private[app] object PipelineExecutor {

  def execute[F[_]: Async](
    resultStream: Stream[F, ProcessingResult],
    logger: PipelineLogger[F],
    pipelineName: String,
    correlationId: UUID,
    workflowStateUpdater: Option[WorkflowStateUpdater[F]] = None,
    workflowRunId: Option[String] = None,
  ): F[ExitCode] = {
    val logCtx = LogContext(runId = correlationId.toString, stepName = pipelineName)
    val runId  = workflowRunId.getOrElse(System.currentTimeMillis().toString)

    for {
      _           <- recordStepStarted(workflowStateUpdater, runId, pipelineName)
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
      _ <- recordStepOutcome(workflowStateUpdater, runId, pipelineName, summary)
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
      s"Pipeline completed: ${summary.itemsProcessed} processed, ${summary.itemsSucceeded} succeeded, ${summary.itemsFailed} failed",
    )

  private def recordStepStarted[F[_]: Async](
    updater: Option[WorkflowStateUpdater[F]],
    runId: String,
    stepName: String,
  ): F[Unit] =
    updater.traverse_(_.recordStepStarted(runId, stepName))

  private def recordStepOutcome[F[_]: Async](
    updater: Option[WorkflowStateUpdater[F]],
    runId: String,
    stepName: String,
    summary: StepRunSummary,
  ): F[Unit] =
    updater.traverse_ { wsu =>
      if (summary.itemsFailed == 0) {
        wsu.recordStepCompleted(runId, stepName)
      } else {
        wsu.recordStepFailed(
          runId,
          stepName,
          s"${summary.itemsFailed} of ${summary.itemsProcessed} items failed",
        )
      }
    }

}
