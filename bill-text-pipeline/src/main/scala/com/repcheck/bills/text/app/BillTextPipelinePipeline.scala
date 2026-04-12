package com.repcheck.bills.text.app

import java.util.UUID

import cats.effect.{Async, ExitCode, Resource}
import cats.syntax.all._

import org.http4s.client.Client

import fs2.Stream

import doobie.util.transactor.Transactor

import repcheck.ingestion.common.db.DatabaseConfig
import repcheck.ingestion.common.events.{DefaultIngestionEventPublisher, EventPublisherConfig, PubSubEventPublisher}
import repcheck.ingestion.common.logging.PipelineLogger
import repcheck.pipeline.models.metadata.ProcessingResult

import com.repcheck.bills.common.persistence.{DoobieBillRepository, DoobieBillTextVersionRepository}
import com.repcheck.bills.text.config.BillTextPipelineConfig
import com.repcheck.bills.text.download.BillTextDownloader
import com.repcheck.bills.text.embedding.{EmbeddingConfig, OllamaEmbeddingService}
import com.repcheck.bills.text.pipeline.BillTextProcessor

private[app] object BillTextPipelinePipeline {

  private val PipelineName = "bill-text-pipeline"

  final case class AppConfig(
    database: DatabaseConfig,
    pipeline: BillTextPipelineConfig,
    eventPublisher: EventPublisherConfig,
    embedding: EmbeddingConfig,
  ) derives pureconfig.ConfigReader

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

  private[app] def buildProcessor[F[_]: Async](
    httpClient: Client[F],
    xa: Transactor[F],
    pubSubPublisher: PubSubEventPublisher[F],
    config: AppConfig,
    logger: PipelineLogger[F],
  ): BillTextProcessor[F] = {
    val billRepository        = new DoobieBillRepository
    val textVersionRepository = new DoobieBillTextVersionRepository
    val downloader            = new BillTextDownloader[F](httpClient, config.pipeline, logger)
    val eventPublisher = new DefaultIngestionEventPublisher[F](
      publisher = pubSubPublisher,
      topicName = config.eventPublisher.topicName,
      source = config.eventPublisher.source,
    )
    val embeddingService = new OllamaEmbeddingService[F](httpClient, config.embedding, logger)

    new BillTextProcessor[F](
      downloader = downloader,
      billRepository = billRepository,
      textVersionRepository = textVersionRepository,
      embeddingService = embeddingService,
      eventPublisher = eventPublisher,
      xa = xa,
      logger = logger,
    )
  }

}
