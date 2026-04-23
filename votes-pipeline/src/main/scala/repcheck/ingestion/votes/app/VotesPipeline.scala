package repcheck.ingestion.votes.app

import cats.effect.{Async, ExitCode, Resource, Sync}
import cats.syntax.all._

import org.http4s.ember.client.EmberClientBuilder

import fs2.Stream
import fs2.io.net.Network

import pureconfig.ConfigSource

import repcheck.ingestion.common.api.CongressGovClientConfig
import repcheck.ingestion.common.db.{DatabaseConfig, TransactorResource}
import repcheck.ingestion.common.events.{EventPublisherConfig, PubSubPublisherResource}
import repcheck.ingestion.common.execution.PipelineBootstrap
import repcheck.ingestion.common.logging.{PipelineLogger, PipelineLoggerFactory}
import repcheck.ingestion.votes.config.VotesPipelineConfig
import repcheck.ingestion.votes.errors.StepRunIdInvalid
import repcheck.ingestion.votes.pipeline.VoteProcessor
import repcheck.pipeline.models.metadata.ProcessingResult

/**
 * Top-level orchestration for the votes pipeline: loads config, extracts the launcher's runId / stepRunId, acquires the
 * logger + managed [[VotesPipelineResources.Resources]] bundle, and hands everything to [[VotesProcessorFactory.build]]
 * to assemble the full [[VoteProcessor]] graph.
 *
 * The file is intentionally small and focused on composition — resource acquisition lives in [[VotesPipelineResources]]
 * and the processor dep-graph wiring lives in [[VotesProcessorFactory]]. Keeping the three concerns in separate files
 * lets each be reviewed and tested as its own logical unit.
 *
 * ==Launcher contract==
 *
 *   - `args(0)` — config JSON placeholder (currently unused; the pipeline loads `application.conf` directly).
 *   - `args(1)` — run-level identifier (`workflow_runs.id` string).
 *   - `args(2)` — step-level identifier (`workflow_run_steps.id` Long assigned by the launcher before invocation).
 */
private[app] object VotesPipeline {

  private val PipelineName = "votes-pipeline"

  /**
   * Top-level application config, derived from `application.conf` via PureConfig auto-derivation. Nests the database /
   * Congress.gov / votes-pipeline / event-publisher sub-configs; individual subprojects own their own case classes and
   * the `derives` machinery composes them.
   */
  final case class AppConfig(
    database: DatabaseConfig,
    congressApi: CongressGovClientConfig,
    pipeline: VotesPipelineConfig,
    eventPublisher: EventPublisherConfig,
  ) derives pureconfig.ConfigReader

  /**
   * Production entry point. Wires real factories and delegates to [[runWithFactories]]. Keep this method as the ONLY
   * place in the codebase that constructs the live GCP / Congress.gov / AlloyDB SDK resources — everything downstream
   * is testable by swapping factories.
   */
  def run[F[_]: Async: Network](args: List[String]): F[ExitCode] =
    runWithFactories[F](
      args = args,
      configLoader = Sync[F].delay(ConfigSource.default.loadOrThrow[AppConfig]),
      loggerFactory = PipelineLoggerFactory.make[F](PipelineName),
      resourceBuilder = (cfg: AppConfig) =>
        VotesPipelineResources.build[F](
          config = cfg,
          transactorFactory = TransactorResource.make[F](_),
          httpClientFactory = EmberClientBuilder.default[F].build,
          pubSubPublisherFactory = PubSubPublisherResource.make[F](_),
        ),
      processorFactory = VotesProcessorFactory.build[F],
      streamFactory = (processor: VoteProcessor[F], runId: String) => processor.streamAll(runId),
    )

  /**
   * Testable runtime. Every collaborator that performs a side effect at app startup is supplied via a factory function.
   * The unit spec uses this to verify ordering (`configLoader` runs once, then `loggerFactory`, then `resourceBuilder`,
   * then `processorFactory`, then `streamFactory`) without constructing any real dependency.
   */
  private[app] def runWithFactories[F[_]: Async](
    args: List[String],
    configLoader: F[AppConfig],
    loggerFactory: F[PipelineLogger[F]],
    resourceBuilder: AppConfig => Resource[F, VotesPipelineResources.Resources[F]],
    processorFactory: (AppConfig, VotesPipelineResources.Resources[F], PipelineLogger[F]) => VoteProcessor[F],
    streamFactory: (VoteProcessor[F], String) => Stream[F, ProcessingResult],
  ): F[ExitCode] =
    for {
      config    <- configLoader
      runId     <- PipelineBootstrap.extractRunId[F](args)
      stepRunId <- extractStepRunId[F](args)
      logger    <- loggerFactory
      exitCode <- resourceBuilder(config).use { resources =>
        val processor = processorFactory(config, resources, logger)
        val stream    = streamFactory(processor, runId)
        PipelineExecutor.execute[F](stream, logger, PipelineName, runId, stepRunId)
      }
    } yield exitCode

  /**
   * Extract the step-level identifier from `args(2)` and parse it as a `Long`. The launcher is responsible for creating
   * the `workflow_run_steps` row and passing its BIGSERIAL PK before invoking the pipeline — a missing or non-numeric
   * value indicates a broken launcher contract and fails the run fast via [[StepRunIdInvalid]].
   */
  private[app] def extractStepRunId[F[_]: Sync](args: List[String]): F[Long] =
    args.lift(2) match {
      case Some(raw) if raw.trim.nonEmpty =>
        raw.trim.toLongOption match {
          case Some(id) => Sync[F].pure(id)
          case None     => Sync[F].raiseError[Long](StepRunIdInvalid(raw))
        }
      case Some(raw) => Sync[F].raiseError[Long](StepRunIdInvalid(raw))
      case None      => Sync[F].raiseError[Long](StepRunIdInvalid("<missing>"))
    }

}
