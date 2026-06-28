package repcheck.ingestion.bills.textcheck.app

import java.util.UUID

import cats.effect.{Async, ExitCode, Resource}
import cats.syntax.all._

import org.http4s.client.Client

import fs2.Stream

import doobie.util.transactor.Transactor

import repcheck.ingestion.bills.common.persistence.DoobieBillRepository
import repcheck.ingestion.bills.textcheck.api.BillTextApiClient
import repcheck.ingestion.bills.textcheck.config.BillTextCheckerConfig
import repcheck.ingestion.bills.textcheck.pipeline.BillTextAvailabilityChecker
import repcheck.ingestion.common.api.CongressGovClientConfig
import repcheck.ingestion.common.db.DatabaseConfig
import repcheck.ingestion.common.events.{DefaultIngestionEventPublisher, EventPublisherConfig, PubSubEventPublisher}
import repcheck.ingestion.common.execution.PipelineExecutor
import repcheck.ingestion.common.logging.PipelineLogger
import repcheck.pipeline.models.metadata.ProcessingResult

import com.repcheck.utils.errors.{ErrorClass, RetryWrapper}

private[app] object BillTextCheckerPipeline {

  private val PipelineName = "bill-text-availability-checker"

  final case class AppConfig(
    database: DatabaseConfig,
    congressApi: CongressGovClientConfig,
    pipeline: BillTextCheckerConfig,
    eventPublisher: EventPublisherConfig,
  ) derives pureconfig.ConfigReader

  /** Resource bundle created by `buildResources` — groups all managed dependencies. */
  final case class CheckerResources[F[_]](
    xa: Transactor[F],
    httpClient: Client[F],
    pubSubPublisher: PubSubEventPublisher[F],
  )

  private[app] def runWithFactories[F[_]: Async](
    configLoader: F[AppConfig],
    loggerFactory: String => F[PipelineLogger[F]],
    resourceBuilder: (AppConfig, PipelineLogger[F]) => Resource[F, CheckerResources[F]],
    checkerFactory: (
      Client[F],
      Transactor[F],
      PubSubEventPublisher[F],
      AppConfig,
      PipelineLogger[F],
    ) => BillTextAvailabilityChecker[F],
    streamFactory: (
      BillTextAvailabilityChecker[F],
      PipelineLogger[F],
      Long,
    ) => Stream[F, ProcessingResult],
    runId: Long,
    stepRunId: Long,
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
   * No-op retry logger used when we don't need per-attempt logging. Extracted as a named method (rather than inlined as
   * a lambda) so tests can invoke it directly and cover its body — an inlined lambda is never exercised in unit tests
   * because the test path doesn't trigger a retry, which leaves the lambda body uncovered.
   */
  private[app] def noOpRetryLogger[F[_]: Async]: (Int, Int, Long, ErrorClass, String, UUID) => F[Unit] =
    (_, _, _, _, _, _) => Async[F].unit

  private[app] def buildChecker[F[_]: Async](
    httpClient: Client[F],
    xa: Transactor[F],
    pubSubPublisher: PubSubEventPublisher[F],
    config: AppConfig,
    logger: PipelineLogger[F],
  ): BillTextAvailabilityChecker[F] = {
    val billRepo     = new DoobieBillRepository
    val retryWrapper = new RetryWrapper[F](noOpRetryLogger[F])
    val eventPublisher = new DefaultIngestionEventPublisher[F](
      publisher = pubSubPublisher,
      topicName = config.eventPublisher.topicName,
      source = config.eventPublisher.source,
      retryWrapper = retryWrapper,
      retryConfig = config.pipeline.eventPublishRetry,
    )

    new BillTextAvailabilityChecker[F](
      textApiClient = new BillTextApiClient[F](config.congressApi, httpClient, retryWrapper),
      billRepo = billRepo,
      eventPublisher = eventPublisher,
      retryWrapper = retryWrapper,
      xa = xa,
      config = config.pipeline,
      logger = logger,
    )
  }

  private[app] def buildStream[F[_]](
    checker: BillTextAvailabilityChecker[F],
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
  ): Resource[F, CheckerResources[F]] = {
    val _ = logger // reserved for future resource-level logging
    for {
      xa              <- transactorFactory(config.database)
      httpClient      <- httpClientFactory
      pubSubPublisher <- pubSubPublisherFactory(config.eventPublisher)
    } yield CheckerResources(xa, httpClient, pubSubPublisher)
  }

}
