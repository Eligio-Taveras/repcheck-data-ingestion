package com.repcheck.bills.textcheck.app

import java.util.UUID

import cats.effect.{Async, ExitCode, Resource, Sync}
import cats.syntax.all._

import org.http4s.client.Client
import org.http4s.ember.client.EmberClientBuilder

import fs2.io.net.Network

import doobie.util.transactor.Transactor

import pureconfig.ConfigSource

import repcheck.ingestion.common.api.CongressGovClientConfig
import repcheck.ingestion.common.db.{DatabaseConfig, TransactorResource}
import repcheck.ingestion.common.events.{
  DefaultIngestionEventPublisher,
  EventPublisherConfig,
  PubSubEventPublisher,
  PubSubPublisherResource,
}
import repcheck.ingestion.common.logging.{PipelineLogger, PipelineLoggerFactory}
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

  def run[F[_]: Async: Network](args: List[String]): F[ExitCode] = {
    val _ = args // args reserved for future CLI config override support
    runWithFactories[F](
      configLoader = Sync[F].delay(ConfigSource.default.loadOrThrow[AppConfig]),
      loggerFactory = (name: String) => PipelineLoggerFactory.make[F](name),
      resourceBuilder = (config: AppConfig) => buildResources[F](config),
      checkerFactory = buildChecker[F],
    )
  }

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
          val checker       = checkerFactory(httpClient, xa, pubSubPublisher, config, logger)
          val correlationId = UUID.randomUUID()
          val resultStream  = checker.checkAll(correlationId)
          PipelineExecutor.execute[F](resultStream, logger, PipelineName, correlationId)
      }
    } yield exitCode

  private def buildChecker[F[_]: Async](
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

  private def buildResources[F[_]: Async: Network](
    config: AppConfig
  ): Resource[F, (Transactor[F], Client[F], PubSubEventPublisher[F])] =
    for {
      xa              <- TransactorResource.make[F](config.database)
      httpClient      <- EmberClientBuilder.default[F].build
      pubSubPublisher <- PubSubPublisherResource.make[F](config.eventPublisher)
    } yield (xa, httpClient, pubSubPublisher)

}
