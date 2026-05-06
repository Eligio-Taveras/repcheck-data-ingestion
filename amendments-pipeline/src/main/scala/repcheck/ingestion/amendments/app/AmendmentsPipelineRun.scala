package repcheck.ingestion.amendments.app

import scala.concurrent.duration._

import cats.effect.{Async, ExitCode, Sync}

import org.http4s.ember.client.EmberClientBuilder

import fs2.io.net.Network

import pureconfig.ConfigSource

import repcheck.ingestion.common.db.TransactorResource
import repcheck.ingestion.common.execution.{PipelineFailureHandlerConfig, WorkflowStateUpdater}
import repcheck.ingestion.common.logging.{PipelineLogger, PipelineLoggerFactory}
import repcheck.pipeline.models.errors.RetryWrapper

/**
 * Production-only entry point. Wires the real `EmberClient` / `HikariTransactor` / `WorkflowStateUpdater` factories and
 * delegates to [[AmendmentsPipeline.runWithFactories]]. Lives in its own file so it stays out of the coverage gate (per
 * the `coverageExcludedFiles` rule for IOApp wiring) while [[AmendmentsPipeline.runWithFactories]] is fully tested.
 *
 * The split mirrors the bill-metadata-pipeline pattern (`BillMetadataPipeline.run` for prod, internal helpers for unit
 * coverage).
 */
private[app] object AmendmentsPipelineRun {

  private val PipelineName = "amendments-pipeline"

  /** Production entry point — builds real factories and hands off to the testable runtime. */
  def run[F[_]: Async: Network](args: List[String]): F[ExitCode] =
    AmendmentsPipeline.runWithFactories[F](
      args = args,
      configLoader = Sync[F].delay(ConfigSource.default.loadOrThrow[AmendmentsPipeline.AppConfig]),
      loggerFactory = PipelineLoggerFactory.make[F](PipelineName),
      retryWrapperFactory = (logger: PipelineLogger[F]) => AmendmentsPipeline.buildRetryWrapper[F](logger),
      resourceBuilder = (cfg: AmendmentsPipeline.AppConfig, retryWrapper: RetryWrapper[F]) =>
        AmendmentsPipelineResources.build[F](
          config = cfg,
          transactorFactory = TransactorResource.make[F](_),
          httpClientFactory = EmberClientBuilder
            .default[F]
            .withTimeout(30.seconds)
            .withIdleConnectionTime(60.seconds)
            .build,
          retryWrapper = retryWrapper,
        ),
      processorFactory = AmendmentsProcessorFactory.build[F],
      streamFactory = (processor, runId) => processor.streamAll(runId),
      stepRecorderFactory = (resources: AmendmentsPipelineResources.Resources[F]) =>
        new WorkflowStateUpdater[F](resources.xa, PipelineFailureHandlerConfig(maxRetries = 3)),
    )

}
