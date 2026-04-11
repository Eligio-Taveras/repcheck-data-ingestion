package com.repcheck.bills.text.app

import java.util.UUID

import cats.effect.{Async, ExitCode, Resource, Sync}
import cats.syntax.all._

import org.http4s.client.Client
import org.http4s.ember.client.EmberClientBuilder

import fs2.Stream
import fs2.io.net.Network

import doobie.util.transactor.Transactor

import pureconfig.ConfigSource

import repcheck.ingestion.common.db.{DatabaseConfig, TransactorResource}
import repcheck.ingestion.common.events.{DefaultIngestionEventPublisher, EventPublisherConfig, PubSubEventPublisher}
import repcheck.ingestion.common.logging.{PipelineLogger, PipelineLoggerFactory}
import repcheck.pipeline.models.metadata.ProcessingResult

import com.repcheck.bills.common.persistence.DoobieBillTextVersionRepository
import com.repcheck.bills.text.config.BillTextPipelineConfig
import com.repcheck.bills.text.download.BillTextDownloader
import com.repcheck.bills.text.pipeline.BillTextProcessor

private[app] object BillTextPipelinePipeline {

  private val PipelineName = "bill-text-pipeline"

  final case class AppConfig(
    database: DatabaseConfig,
    pipeline: BillTextPipelineConfig,
    eventPublisher: EventPublisherConfig,
  ) derives pureconfig.ConfigReader

  def run[F[_]: Async: Network](args: List[String]): F[ExitCode] = {
    val _ = args // args reserved for future CLI config override support
    runWithFactories[F](
      configLoader = Sync[F].delay(ConfigSource.default.loadOrThrow[AppConfig]),
      loggerFactory = (name: String) => PipelineLoggerFactory.make[F](name),
      resourceBuilder = (config: AppConfig) => buildResources[F](config),
      processorFactory = buildProcessor[F],
    )
  }

  private[app] def runWithFactories[F[_]: Async](
    configLoader: F[AppConfig],
    loggerFactory: String => F[PipelineLogger[F]],
    resourceBuilder: AppConfig => Resource[F, (Transactor[F], Client[F], PubSubEventPublisher[F])],
    processorFactory: (
      Client[F],
      Transactor[F],
      PubSubEventPublisher[F],
      AppConfig,
      PipelineLogger[F],
    ) => BillTextProcessor[F],
  ): F[ExitCode] =
    for {
      config <- configLoader
      logger <- loggerFactory(PipelineName)
      exitCode <- resourceBuilder(config).use {
        case (xa, httpClient, pubSubPublisher) =>
          val processor     = processorFactory(httpClient, xa, pubSubPublisher, config, logger)
          val correlationId = UUID.randomUUID()
          // Event-driven pipeline: in production, events come from a Pub/Sub subscription.
          // For now, the stream is empty — subscription wiring will be added in a future PR.
          val resultStream: Stream[F, ProcessingResult] = Stream.empty
          val _ = processor // processor will be used when subscription wiring is added
          PipelineExecutor.execute[F](resultStream, logger, PipelineName, correlationId)
      }
    } yield exitCode

  private def buildProcessor[F[_]: Async](
    httpClient: Client[F],
    xa: Transactor[F],
    pubSubPublisher: PubSubEventPublisher[F],
    config: AppConfig,
    logger: PipelineLogger[F],
  ): BillTextProcessor[F] = {
    val repository = new DoobieBillTextVersionRepository
    val downloader = new BillTextDownloader[F](httpClient, config.pipeline, logger)
    val eventPublisher = new DefaultIngestionEventPublisher[F](
      publisher = pubSubPublisher,
      topicName = config.eventPublisher.topicName,
      source = config.eventPublisher.source,
    )

    new BillTextProcessor[F](
      downloader = downloader,
      repository = repository,
      eventPublisher = eventPublisher,
      xa = xa,
      logger = logger,
    )
  }

  private def buildResources[F[_]: Async: Network](
    config: AppConfig
  ): Resource[F, (Transactor[F], Client[F], PubSubEventPublisher[F])] =
    for {
      xa         <- TransactorResource.make[F](config.database)
      httpClient <- EmberClientBuilder.default[F].build
    } yield {
      // PubSubEventPublisher is a trait — in production, a concrete implementation would be
      // provided here. For now, a no-op stub is used as the Pub/Sub infrastructure is not
      // yet wired. This will be replaced when the Pub/Sub resource builder is implemented.
      val pubSubPublisher = new PubSubEventPublisher[F] {
        def publish(topic: String, data: String, attributes: Map[String, String]): F[String] =
          Async[F].pure(s"stub-message-id-${UUID.randomUUID()}")
      }
      (xa, httpClient, pubSubPublisher)
    }

}
