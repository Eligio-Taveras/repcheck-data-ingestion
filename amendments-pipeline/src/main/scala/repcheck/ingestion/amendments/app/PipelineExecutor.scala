package repcheck.ingestion.amendments.app

import cats.effect.{Async, ExitCode}
import cats.syntax.all._

import fs2.Stream

import repcheck.ingestion.amendments.observability.AmendmentMetrics
import repcheck.ingestion.amendments.pipeline.AmendmentProcessor
import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}
import repcheck.pipeline.models.metadata.ProcessingResult

/**
 * Testable pipeline execution logic. Accepts a pre-built result stream + processor (used solely for `summarize`) and a
 * logger so tests can inject stubs without constructing the full dependency graph. Mirrors the `bill-metadata-pipeline`
 * executor pattern with the addition of a `summarize`-driven info log + metrics snapshot at the tail.
 *
 * Returns `ExitCode.Success` when no item failed; `ExitCode.Error` otherwise. Skipped is treated as a successful no-op
 * for exit-code purposes (per pipeline-models v0.1.21 — idempotent skip is success).
 */
private[app] object PipelineExecutor {

  def execute[F[_]: Async](
    resultStream: Stream[F, ProcessingResult],
    processor: AmendmentProcessor[F],
    logger: PipelineLogger[F],
    pipelineName: String,
    runId: String,
    metrics: AmendmentMetrics,
  ): F[ExitCode] = {
    val ctx = LogContext(runId, pipelineName)
    for {
      _       <- logger.info(ctx, s"PipelineExecutor.execute.start runId=$runId pipelineName=$pipelineName")
      results <- resultStream.compile.toList
      summary  = processor.summarize(results)
      snapshot = metrics.snapshot()
      _ <- logger.info(
        ctx,
        s"Pipeline completed: total=${summary.totalProcessed.toString} " +
          s"succeeded=${summary.succeeded.toString} skipped=${summary.skipped.toString} " +
          s"failed=${summary.failed.toString} eventsEmitted=${summary.eventsEmitted.toString}",
      )
      _ <- logger.info(
        ctx,
        s"Metrics: detailFetches=${snapshot.detailFetches.toString} " +
          s"recursionRedundant=${snapshot.recursionRedundantFetches.toString} " +
          s"orphanResolved=${snapshot.orphanResolved.toString} " +
          s"recursionDepthExceeded=${snapshot.recursionDepthExceeded.toString}",
      )
      _ <-
        if (summary.errors.nonEmpty) {
          logger.warn(
            ctx,
            s"Pipeline failures: ${summary.errors.take(20).mkString("; ")}" +
              (if (summary.errors.size > 20) { s" (+${(summary.errors.size - 20).toString} more)" }
               else { "" }),
          )
        } else {
          Async[F].unit
        }
      exitCode =
        if (summary.failed == 0) { ExitCode.Success }
        else { ExitCode.Error }
      _ <- logger.info(ctx, s"PipelineExecutor.execute.exit exitCode=${exitCode.toString}")
    } yield exitCode
  }

}
