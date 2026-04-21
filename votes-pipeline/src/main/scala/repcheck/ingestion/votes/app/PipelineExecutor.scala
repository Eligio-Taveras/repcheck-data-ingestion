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
 * operator. `StepProgress` retains the actual `ProcessingResult.Failed` instances (full reason + error class +
 * entityId) so the summary log can include failure details useful for debugging rather than just anonymous counts.
 * Succeeded and skipped results contribute counters only since their per-item context is not debug-relevant at summary
 * time.
 */
private[app] object PipelineExecutor {

  /**
   * Per-stream-step progress accumulator. Contributed one-per-result by [[StepProgress.fromResult]] and combined via
   * the [[StepProgress.monoid]] instance. Keeps full failure context (one `ProcessingResult.Failed` per failure) so the
   * summary log surfaces actual error information; succeeded and skipped results are counted but not retained.
   */
  final private case class StepProgress(
    succeededCount: Int,
    skippedCount: Int,
    failures: Vector[ProcessingResult.Failed],
  ) {
    def itemsProcessed: Int = succeededCount + skippedCount + failures.length
    def itemsFailed: Int    = failures.length
    def itemsSucceeded: Int = succeededCount

    /**
     * Group failures by reason and count each. Derived from the retained failure list rather than tracked in parallel,
     * so the aggregation stays in sync with the actual failures the pipeline produced.
     */
    def errorCounts: Map[String, Int] =
      failures.groupBy(_.reason).view.mapValues(_.length).toMap

  }

  private object StepProgress {

    val empty: StepProgress = StepProgress(0, 0, Vector.empty)

    def fromResult(result: ProcessingResult): StepProgress =
      result match {
        case _: ProcessingResult.Succeeded => StepProgress(1, 0, Vector.empty)
        case _: ProcessingResult.Skipped   => StepProgress(0, 1, Vector.empty)
        case f: ProcessingResult.Failed    => StepProgress(0, 0, Vector(f))
      }

    implicit val monoid: Monoid[StepProgress] = new Monoid[StepProgress] {
      override def empty: StepProgress = StepProgress.empty

      override def combine(a: StepProgress, b: StepProgress): StepProgress =
        StepProgress(
          succeededCount = a.succeededCount + b.succeededCount,
          skippedCount = a.skippedCount + b.skippedCount,
          failures = a.failures ++ b.failures,
        )
    }

  }

  /**
   * @param runId
   *   run-level identifier for this pipeline execution, sourced from the launcher (or equivalent entry point) via
   *   [[repcheck.ingestion.common.execution.PipelineBootstrap.extractRunId]]. Per-item correlation IDs are generated
   *   independently by downstream processors for per-vote log search.
   * @param stepRunId
   *   step-level identifier — the `workflow_run_steps.id` BIGSERIAL PK supplied by the launcher alongside `runId`.
   *   Wired through the `StepRunSummary` so downstream consumers (WorkflowStateUpdater completions, event payloads) can
   *   key off the same row the launcher created.
   */
  def execute[F[_]: Async](
    resultStream: Stream[F, ProcessingResult],
    logger: PipelineLogger[F],
    pipelineName: String,
    runId: String,
    stepRunId: Long,
  ): F[ExitCode] = {
    val logCtx = LogContext(runId = runId, stepName = pipelineName)

    for {
      startedAt <- Async[F].realTimeInstant
      progress <- resultStream
        .map(StepProgress.fromResult)
        .compile
        .foldMonoid
      completedAt <- Async[F].realTimeInstant
      summary = buildSummary(stepRunId, pipelineName, progress, startedAt, completedAt)
      _ <- logger.info(logCtx, summaryMessage(summary))
      _ <- Async[F].whenA(progress.failures.nonEmpty)(
        logger.info(logCtx, failureDetailsMessage(progress.failures))
      )
      exitCode =
        if (summary.itemsFailed == 0) { ExitCode.Success }
        else { ExitCode.Error }
    } yield exitCode
  }

  private def buildSummary(
    stepRunId: Long,
    pipelineName: String,
    progress: StepProgress,
    startedAt: Instant,
    completedAt: Instant,
  ): StepRunSummary =
    StepRunSummary(
      stepRunId = stepRunId,
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

  private def summaryMessage(summary: StepRunSummary): String =
    s"Pipeline completed: ${summary.itemsProcessed} processed, " +
      s"${summary.itemsSucceeded} succeeded, ${summary.itemsFailed} failed"

  /**
   * Render the full failure list on a second log line so debugging doesn't have to cross-reference counts back to
   * individual per-vote log entries. Each entry carries the entityId (e.g. vote natural key), the error classifier, and
   * the reason — matching what the processor captured when it raised the failure.
   */
  private def failureDetailsMessage(failures: Vector[ProcessingResult.Failed]): String = {
    val entries =
      failures.map(f => s"${f.entityId}(${f.errorClass}): ${f.reason}").mkString("; ")
    s"Failure details: $entries"
  }

}
