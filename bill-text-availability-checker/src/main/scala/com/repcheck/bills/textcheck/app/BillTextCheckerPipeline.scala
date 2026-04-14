package com.repcheck.bills.textcheck.app

import cats.effect.{Async, ExitCode, Resource}
import cats.syntax.all._

import org.http4s.client.Client

import fs2.Stream

import doobie.util.transactor.Transactor

import repcheck.ingestion.common.api.CongressGovClientConfig
import repcheck.ingestion.common.db.DatabaseConfig
import repcheck.ingestion.common.events.{DefaultIngestionEventPublisher, EventPublisherConfig, PubSubEventPublisher}
import repcheck.ingestion.common.logging.PipelineLogger
import repcheck.pipeline.models.errors.RetryWrapper
import repcheck.pipeline.models.metadata.ProcessingResult

import com.repcheck.bills.common.persistence.DoobieBillRepository
import com.repcheck.bills.textcheck.api.BillTextApiClient
import com.repcheck.bills.textcheck.config.BillTextCheckerConfig
import com.repcheck.bills.textcheck.pipeline.BillTextAvailabilityChecker

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
    ) => Stream[F, ProcessingResult],
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
        val resultStream = streamFactory(checker, logger)
        PipelineExecutor.execute[F](
          resultStream = resultStream,
          logger = logger,
          pipelineName = PipelineName,
          correlationId = java.util.UUID.randomUUID(),
        )
      }
    } yield exitCode

  private[app] def buildChecker[F[_]: Async](
    httpClient: Client[F],
    xa: Transactor[F],
    pubSubPublisher: PubSubEventPublisher[F],
    config: AppConfig,
    logger: PipelineLogger[F],
  ): BillTextAvailabilityChecker[F] = {
    val billRepo     = new DoobieBillRepository
    val retryWrapper = new RetryWrapper[F]((_, _, _, _, _, _) => Async[F].unit)
    val eventPublisher = new DefaultIngestionEventPublisher[F](
      publisher = pubSubPublisher,
      topicName = config.eventPublisher.topicName,
      source = config.eventPublisher.source,
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
  ): Stream[F, ProcessingResult] = {
    val _             = logger // reserved for future pre/post-stream logging
    val correlationId = java.util.UUID.randomUUID()
    checker.checkAll(correlationId)
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
