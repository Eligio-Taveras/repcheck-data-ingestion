package repcheck.ingestion.amendments.textcheck.app

import java.util.UUID

import cats.effect.{Async, ExitCode, Resource}
import cats.syntax.all._

import org.http4s.client.Client

import fs2.Stream

import doobie.util.transactor.Transactor

import repcheck.ingestion.amendments.persistence.DoobieAmendmentRepository
import repcheck.ingestion.amendments.textcheck.api.AmendmentTextApiClient
import repcheck.ingestion.amendments.textcheck.config.AmendmentTextCheckerConfig
import repcheck.ingestion.amendments.textcheck.events.DefaultAmendmentTextEventPublisher
import repcheck.ingestion.amendments.textcheck.persistence.DoobieAmendmentTextVersionLookup
import repcheck.ingestion.amendments.textcheck.pipeline.AmendmentTextAvailabilityChecker
import repcheck.ingestion.common.api.CongressGovClientConfig
import repcheck.ingestion.common.db.DatabaseConfig
import repcheck.ingestion.common.events.{EventPublisherConfig, PubSubEventPublisher}
import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}
import repcheck.pipeline.models.errors.{ErrorClass, RetryWrapper}
import repcheck.pipeline.models.metadata.ProcessingResult

/**
 * Pure-wiring entry point for the amendment-text-availability-checker IOApp. Each method is a `private[app]` factory
 * function so the `AmendmentTextCheckerApp` IOApp itself stays a one-liner of dependency construction — the testability
 * refactor in CLAUDE.md applied byte-for-byte from the bill-side `BillTextCheckerPipeline`.
 *
 * Coverage-excluded because every code path is a wiring concern (config load, resource build, checker factory, stream
 * factory, executor delegate); domain logic lives in
 * [[repcheck.ingestion.amendments.textcheck.pipeline.AmendmentTextAvailabilityChecker]] and is fully unit-tested there.
 */
private[app] object AmendmentTextCheckerRun {

  private val PipelineName = "amendment-text-availability-checker"

  final case class AppConfig(
    database: DatabaseConfig,
    congressApi: CongressGovClientConfig,
    pipeline: AmendmentTextCheckerConfig,
    eventPublisher: EventPublisherConfig,
  ) derives pureconfig.ConfigReader

  /**
   * @param runId
   *   workflow-run identifier passed from the IOApp's CLI args. Defaults to `0L` when absent — placeholder until
   *   `workflow_runs` registration is wired up. Threaded into `buildStream` and `PipelineExecutor.execute`.
   * @param stepRunId
   *   workflow_run_steps row identifier from the IOApp's CLI args. Defaults to `0L` — placeholder until
   *   `workflow_run_steps` registration is wired up.
   */
  private[app] def runWithFactories[F[_]: Async](
    configLoader: F[AppConfig],
    loggerFactory: String => F[PipelineLogger[F]],
    resourceBuilder: (AppConfig, PipelineLogger[F]) => Resource[F, AmendmentTextCheckerResources[F]],
    checkerFactory: (
      Client[F],
      Transactor[F],
      PubSubEventPublisher[F],
      AppConfig,
      PipelineLogger[F],
    ) => AmendmentTextAvailabilityChecker[F],
    streamFactory: (
      AmendmentTextAvailabilityChecker[F],
      PipelineLogger[F],
      Long,
    ) => Stream[F, ProcessingResult],
    runId: Long = 0L,
    stepRunId: Long = 0L,
  ): F[ExitCode] =
    for {
      config <- configLoader
      logger <- loggerFactory(PipelineName)
      exitCode <- resourceBuilder(config, logger).use { resources =>
        val checker = checkerFactory(
          resources.httpClient,
          resources.xa,
          resources.pubSubPublisher,
          config,
          logger,
        )
        val resultStream = streamFactory(checker, logger, runId)
        PipelineExecutor.execute[F](
          resultStream = resultStream,
          logger = logger,
          pipelineName = PipelineName,
          runId = runId,
          stepRunId = stepRunId,
        )
      }
    } yield exitCode

  /**
   * No-op retry logger — used by tests that don't want retry warnings polluting captured output. Extracted as a named
   * method (rather than inlined as a lambda) so tests can invoke it directly and cover its body.
   */
  private[app] def noOpRetryLogger[F[_]: Async]: (Int, Int, Long, ErrorClass, String, UUID) => F[Unit] =
    (_, _, _, _, _, _) => Async[F].unit

  /**
   * Per-attempt retry logger used in production. Logs each retry as a `WARN` with the attempt number, max-attempts cap,
   * scheduled delay, error classification, and originating message. Correlation ID is attached so the warning lines up
   * with the surrounding per-amendment work in log aggregation.
   *
   * Constructed in `buildChecker` with the live [[PipelineLogger]] so retries emit through the same backend as the rest
   * of the pipeline (structured JSON in production, line-formatted in tests).
   */
  private[app] def loggingRetryLogger[F[_]](
    logger: PipelineLogger[F],
    stepName: String,
  ): (Int, Int, Long, ErrorClass, String, UUID) => F[Unit] =
    (attempt, maxAttempts, delayMs, errorClass, errorMessage, correlationId) =>
      logger.warn(
        LogContext(
          runId = correlationId.toString,
          stepName = stepName,
          correlationId = Some(correlationId),
        ),
        s"Retry ${attempt.toString}/${maxAttempts.toString} in ${delayMs.toString}ms after " +
          s"${errorClass.toString} error: $errorMessage",
      )

  private[app] def buildChecker[F[_]: Async](
    httpClient: Client[F],
    xa: Transactor[F],
    pubSubPublisher: PubSubEventPublisher[F],
    config: AppConfig,
    logger: PipelineLogger[F],
  ): AmendmentTextAvailabilityChecker[F] = {
    val amendmentRepo     = new DoobieAmendmentRepository
    val textVersionLookup = new DoobieAmendmentTextVersionLookup
    val retryWrapper      = new RetryWrapper[F](loggingRetryLogger[F](logger, PipelineName))
    val eventPublisher = new DefaultAmendmentTextEventPublisher[F](
      publisher = pubSubPublisher,
      topicName = config.eventPublisher.topicName,
      source = config.eventPublisher.source,
      retryWrapper = retryWrapper,
      retryConfig = config.pipeline.eventPublishRetry,
    )

    new AmendmentTextAvailabilityChecker[F](
      apiClient = new AmendmentTextApiClient[F](config.congressApi, httpClient, retryWrapper),
      amendmentRepo = amendmentRepo,
      textVersionLookup = textVersionLookup,
      eventPublisher = eventPublisher,
      xa = xa,
      config = config.pipeline,
      logger = logger,
    )
  }

  private[app] def buildStream[F[_]](
    checker: AmendmentTextAvailabilityChecker[F],
    logger: PipelineLogger[F],
    runId: Long,
  ): Stream[F, ProcessingResult] = {
    val _ = logger // reserved for future pre/post-stream logging
    checker.checkAll(runId)
  }

  private[app] def buildResources[F[_]](
    config: AppConfig,
    logger: PipelineLogger[F],
    transactorFactory: DatabaseConfig => Resource[F, Transactor[F]],
    httpClientFactory: Resource[F, Client[F]],
    pubSubPublisherFactory: EventPublisherConfig => Resource[F, PubSubEventPublisher[F]],
  ): Resource[F, AmendmentTextCheckerResources[F]] =
    AmendmentTextCheckerResources.build[F](
      config = config,
      logger = logger,
      transactorFactory = transactorFactory,
      httpClientFactory = httpClientFactory,
      pubSubPublisherFactory = pubSubPublisherFactory,
    )

}
