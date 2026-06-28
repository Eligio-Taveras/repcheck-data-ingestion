package repcheck.ingestion.amendments.app

import cats.effect.{Async, ExitCode, Resource}
import cats.syntax.all._

import fs2.Stream

import repcheck.ingestion.amendments.config.AmendmentsConfig
import repcheck.ingestion.amendments.errors.PoolSizingTooSmall
import repcheck.ingestion.amendments.observability.AmendmentMetrics
import repcheck.ingestion.amendments.pipeline.{AmendmentProcessor, VoteAmendmentLinker}
import repcheck.ingestion.common.api.CongressGovClientConfig
import repcheck.ingestion.common.db.DatabaseConfig
import repcheck.ingestion.common.execution.{PipelineBootstrap, PipelineExecutor, WorkflowStateUpdater}
import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}
import repcheck.pipeline.models.metadata.ProcessingResult

import com.repcheck.utils.errors.RetryWrapper

/**
 * Top-level orchestration for the amendments pipeline: loads config, extracts the launcher's runId, acquires the logger
 * + managed [[AmendmentsPipelineResources.Resources]] bundle, hands everything to [[AmendmentsProcessorFactory.build]],
 * runs the result stream through [[PipelineExecutor.execute]], and finally stamps `workflow_run_steps` via
 * [[WorkflowStateUpdater.recordStepCompleted]].
 *
 * Mirrors the votes-pipeline `VotesPipeline.run` / `runWithFactories` split: the production entry point wires real
 * factories; tests inject stubs through `runWithFactories` to verify ordering and threading without touching real GCP /
 * Congress.gov / AlloyDB resources.
 *
 * ==Connection-pool sizing (per P1)==
 *
 * `application.conf` defaults `database.max-connections` to 45 — `parallelism × maxRecursionDepth + 5 = 4 × 10 + 5`.
 * The bootstrap validates this at boot before opening the transactor; an undersized pool deadlocks a deep cold chain
 * during recursion (every pending sponsor / bill / parent resolution holds one connection). Failing fast at boot keeps
 * the misconfiguration loud.
 */
private[app] object AmendmentsPipeline {

  private val PipelineName = "amendments-pipeline"

  /**
   * Top-level application config, derived from `application.conf` via PureConfig auto-derivation.
   */
  final case class AppConfig(
    database: DatabaseConfig,
    congressApi: CongressGovClientConfig,
    pipeline: AmendmentsConfig,
  ) derives pureconfig.ConfigReader

  /**
   * Testable runtime. Every collaborator that performs a side effect at app startup is supplied via a factory function.
   * The unit spec uses this to verify ordering and threading without constructing any real dependency.
   */
  private[app] def runWithFactories[F[_]: Async](
    args: List[String],
    configLoader: F[AppConfig],
    loggerFactory: F[PipelineLogger[F]],
    retryWrapperFactory: PipelineLogger[F] => RetryWrapper[F],
    resourceBuilder: (AppConfig, RetryWrapper[F]) => Resource[F, AmendmentsPipelineResources.Resources[F]],
    processorFactory: (
      AppConfig,
      AmendmentsPipelineResources.Resources[F],
      PipelineLogger[F],
      AmendmentMetrics,
    ) => AmendmentProcessor[F],
    streamFactory: (AmendmentProcessor[F], String) => Stream[F, ProcessingResult],
    stepRecorderFactory: AmendmentsPipelineResources.Resources[F] => WorkflowStateUpdater[F],
    linkerFactory: (AmendmentsPipelineResources.Resources[F], PipelineLogger[F]) => VoteAmendmentLinker[F],
  ): F[ExitCode] =
    for {
      config    <- configLoader
      _         <- validatePoolSizingF[F](config)
      runId     <- PipelineBootstrap.extractRunId[F](args)
      stepRunId <- PipelineBootstrap.extractStepRunId[F](args)
      logger    <- loggerFactory
      _ <- logger.info(
        LogContext(runId.toString, PipelineName),
        s"Boot: db=${config.database.host}:${config.database.port.toString}/${config.database.database} " +
          s"maxConnections=${config.database.maxConnections.toString} " +
          s"pipeline.parallelism=${config.pipeline.parallelism.toString} " +
          s"pipeline.maxRecursionDepth=${config.pipeline.maxRecursionDepth.toString} " +
          s"pipeline.lookbackDays=${config.pipeline.lookbackDays.toString} " +
          s"pipeline.congresses=${config.pipeline.congressesMin.toString}..${config.pipeline.congressesMax.toString}",
      )
      retryWrapper = retryWrapperFactory(logger)
      metrics      = AmendmentMetrics.make()
      exitCode <- resourceBuilder(config, retryWrapper).use { resources =>
        val processor    = processorFactory(config, resources, logger, metrics)
        val stepRecorder = stepRecorderFactory(resources)
        val linker       = linkerFactory(resources, logger)
        val stream       = streamFactory(processor, runId.toString)
        for {
          _    <- stepRecorder.recordStepStarted(runId, PipelineName)
          code <- PipelineExecutor.execute[F](stream, logger, PipelineName, runId, stepRunId)
          _    <- logMetricsSnapshot[F](logger, runId.toString, metrics)
          _ <-
            if (code == ExitCode.Success) {
              // Reconcile votes↔amendments from the freshly-upserted action text, then record completion. The link
              // pass is best-effort: a failure here is logged but does not fail an otherwise-successful run.
              runLinker[F](linker, runId.toString, logger) *> stepRecorder.recordStepCompleted(runId, PipelineName)
            } else {
              stepRecorder.recordStepFailed(runId, PipelineName, "pipeline reported one or more failures")
            }
        } yield code
      }
    } yield exitCode

  private def logMetricsSnapshot[F[_]](
    logger: PipelineLogger[F],
    runId: String,
    metrics: AmendmentMetrics,
  ): F[Unit] = {
    val snapshot = metrics.snapshot()
    logger.info(
      LogContext(runId, PipelineName),
      s"Metrics: detailFetches=${snapshot.detailFetches.toString} " +
        s"recursionRedundant=${snapshot.recursionRedundantFetches.toString} " +
        s"orphanResolved=${snapshot.orphanResolved.toString} " +
        s"recursionDepthExceeded=${snapshot.recursionDepthExceeded.toString}",
    )
  }

  /**
   * P1 pool-sizing pre-flight check, lifted into `F[_]` so the failure flows through the same effect chain as the rest
   * of `runWithFactories`. Raises [[PoolSizingTooSmall]] with the diagnostic message produced by
   * [[AmendmentsPipelineResources.validatePoolSizing]].
   */
  private[app] def validatePoolSizingF[F[_]: Async](config: AppConfig): F[Unit] =
    AmendmentsPipelineResources.validatePoolSizing(config.database, config.pipeline) match {
      case Some(message) => Async[F].raiseError[Unit](PoolSizingTooSmall(message))
      case None          => Async[F].unit
    }

  /**
   * Run the vote↔amendment reconciliation as a non-fatal post-pass. The amendments run has already succeeded by the
   * time this fires; a linker failure (e.g. a transient DB hiccup) is logged and swallowed so it can't flip an
   * otherwise-green run to failed. The link statement is idempotent, so the next run reconciles whatever this pass
   * missed.
   */
  private[app] def runLinker[F[_]: Async](
    linker: VoteAmendmentLinker[F],
    runId: String,
    logger: PipelineLogger[F],
  ): F[Unit] = {
    val ctx = LogContext(runId, PipelineName)
    linker.linkAll(ctx).void.handleErrorWith { err =>
      logger.error(ctx, s"vote-amendment-linker failed (non-fatal): ${err.getMessage}", Some(err))
    }
  }

  /**
   * Build a `RetryWrapper[F]` whose log callback funnels every retry attempt into the pipeline's structured logger as a
   * WARN. Mirrors `BillMetadataPipeline.run` — without this the wrapper is a black hole on retries, and a stalled Ember
   * timeout looks like a frozen pipeline to operators.
   */
  private[app] def buildRetryWrapper[F[_]: Async](logger: PipelineLogger[F]): RetryWrapper[F] = {
    val ctx = LogContext("0", PipelineName)
    new RetryWrapper[F]((attempt, maxRetries, delayMs, errorClass, message, correlationId) =>
      logger.warn(
        ctx.copy(correlationId = Some(correlationId)),
        s"Retry ${attempt.toString}/${maxRetries.toString} scheduled in ${delayMs.toString}ms " +
          s"(errorClass=${errorClass.toString}): $message",
      )
    )
  }

}
