package com.repcheck.bills.textcheck.app

import java.util.UUID

import cats.effect.{Async, ExitCode, Resource}
import cats.syntax.all._

import org.http4s.client.Client

import doobie.util.transactor.Transactor

import repcheck.ingestion.common.api.CongressGovClientConfig
import repcheck.ingestion.common.db.DatabaseConfig
import repcheck.ingestion.common.events.{DefaultIngestionEventPublisher, EventPublisherConfig, PubSubEventPublisher}
import repcheck.ingestion.common.logging.PipelineLogger
import repcheck.pipeline.models.errors.RetryWrapper

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

  private[app] def runWithFactories[F[_]: Async](
    configLoader: F[AppConfig],
    loggerFactory: String => F[PipelineLogger[F]],
    resourceBuilder: AppConfig => Resource[F, (Transactor[F], Client[F], PubSubEventPublisher[F])],
    checkerFactory: (
      Client[F],
      Transactor[F],
      PubSubEventPublisher[F],
      AppConfig,
      PipelineLogger[F],
    ) => BillTextAvailabilityChecker[F],
  ): F[ExitCode] =
    for {
      config <- configLoader
      logger <- loggerFactory(PipelineName)
      exitCode <- resourceBuilder(config).use {
        case (xa, httpClient, pubSubPublisher) =>
          val checker      = checkerFactory(httpClient, xa, pubSubPublisher, config, logger)
          val runId        = UUID.randomUUID()
          val resultStream = checker.checkAll(runId)
          PipelineExecutor.execute[F](resultStream, logger, PipelineName, runId)
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
    val apiClient    = new BillTextApiClient[F](config.congressApi, httpClient, retryWrapper)
    val eventPublisher = new DefaultIngestionEventPublisher[F](
      publisher = pubSubPublisher,
      topicName = config.eventPublisher.topicName,
      source = config.eventPublisher.source,
    )

    new BillTextAvailabilityChecker[F](
      textApiClient = apiClient,
      billRepo = billRepo,
      eventPublisher = eventPublisher,
      retryWrapper = retryWrapper,
      xa = xa,
      config = config.pipeline,
      logger = logger,
    )
  }

}
