package com.repcheck.bills.text.app

import cats.effect.{Async, ExitCode, Resource, Sync}
import cats.syntax.all._

import org.http4s.client.Client
import org.http4s.ember.client.EmberClientBuilder

import fs2.Stream
import fs2.io.net.Network

import doobie.util.transactor.Transactor

import pureconfig.ConfigSource

import repcheck.ingestion.common.db.{DatabaseConfig, TransactorResource}
import repcheck.ingestion.common.events.{
  DefaultIngestionEventPublisher,
  EventPublisherConfig,
  PubSubEventPublisher,
  PubSubPublisherResource,
}
import repcheck.ingestion.common.execution.{PipelineFailureHandlerConfig, WorkflowStateUpdater}
import repcheck.ingestion.common.logging.{PipelineLogger, PipelineLoggerFactory}
import repcheck.pipeline.models.metadata.ProcessingResult

import com.repcheck.bills.common.persistence.{DoobieBillRepository, DoobieBillTextVersionRepository}
import com.repcheck.bills.text.config.BillTextPipelineConfig
import com.repcheck.bills.text.download.BillTextDownloader
import com.repcheck.bills.text.embedding.{EmbeddingConfig, OllamaEmbeddingService}
import com.repcheck.bills.text.pipeline.BillTextProcessor
import com.repcheck.bills.text.subscription.{
  EventSubscriberConfig,
  PubSubEventSubscriber,
  PubSubSubscriberResource,
  ReceivedEvent,
}

private[app] object BillTextPipelinePipeline {

  private val PipelineName = "bill-text-pipeline"

  final case class AppConfig(
    database: DatabaseConfig,
    pipeline: BillTextPipelineConfig,
    eventPublisher: EventPublisherConfig,
    eventSubscriber: EventSubscriberConfig,
    embedding: EmbeddingConfig,
    failureHandler: PipelineFailureHandlerConfig,
  ) derives pureconfig.ConfigReader

  /** Resource bundle created by `buildResources` — groups all managed dependencies. */
  final case class PipelineResources[F[_]](
    xa: Transactor[F],
    httpClient: Client[F],
    pubSubPublisher: PubSubEventPublisher[F],
    pubSubSubscriber: PubSubEventSubscriber[F],
  )

  def run[F[_]: Async: Network](args: List[String]): F[ExitCode] = {
    val _ = args // args reserved for future CLI config override support
    runWithFactories[F](
      configLoader = Sync[F].delay(ConfigSource.default.loadOrThrow[AppConfig]),
      loggerFactory = (name: String) => PipelineLoggerFactory.make[F](name),
      resourceBuilder = (config: AppConfig, logger: PipelineLogger[F]) => buildResources[F](config, logger),
      processorFactory = buildProcessor[F],
      streamFactory = buildStream[F],
      workflowStateUpdaterFactory = (xa, cfg) => Some(new WorkflowStateUpdater[F](xa, cfg)),
    )
  }

  private[app] def runWithFactories[F[_]: Async](
    configLoader: F[AppConfig],
    loggerFactory: String => F[PipelineLogger[F]],
    resourceBuilder: (AppConfig, PipelineLogger[F]) => Resource[F, PipelineResources[F]],
    processorFactory: (
      Client[F],
      Transactor[F],
      PubSubEventPublisher[F],
      AppConfig,
      PipelineLogger[F],
    ) => BillTextProcessor[F],
    streamFactory: (
      PubSubEventSubscriber[F],
      BillTextProcessor[F],
      AppConfig,
      PipelineLogger[F],
    ) => Stream[F, ProcessingResult],
    workflowStateUpdaterFactory: (Transactor[F], PipelineFailureHandlerConfig) => Option[WorkflowStateUpdater[F]],
  ): F[ExitCode] =
    for {
      config <- configLoader
      logger <- loggerFactory(PipelineName)
      exitCode <- resourceBuilder(config, logger).use { resources =>
        val processor = processorFactory(
          resources.httpClient,
          resources.xa,
          resources.pubSubPublisher,
          config,
          logger,
        )
        val workflowStateUpdater = workflowStateUpdaterFactory(resources.xa, config.failureHandler)
        val resultStream         = streamFactory(resources.pubSubSubscriber, processor, config, logger)
        PipelineExecutor.execute[F](
          resultStream = resultStream,
          logger = logger,
          pipelineName = PipelineName,
          correlationId = java.util.UUID.randomUUID(),
          workflowStateUpdater = workflowStateUpdater,
        )
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

  /**
   * Builds the FS2 result stream from the Pub/Sub subscriber. Pulls a batch of messages, deserializes each into a
   * `PipelineEvent[BillTextAvailableEvent]`, processes via the `BillTextProcessor`, and acknowledges successfully
   * processed messages. The correlationId is extracted from each `PipelineEvent` envelope — one correlationId per bill.
   */
  private[app] def buildStream[F[_]: Async](
    subscriber: PubSubEventSubscriber[F],
    processor: BillTextProcessor[F],
    config: AppConfig,
    logger: PipelineLogger[F],
  ): Stream[F, ProcessingResult] =
    Stream
      .eval(subscriber.pull(config.eventSubscriber.maxMessages))
      .flatMap(Stream.emits)
      .parEvalMap(config.pipeline.parallelism) { receivedEvent =>
        processAndAck(subscriber, processor, receivedEvent, logger)
      }

  private[app] def processAndAck[F[_]: Async](
    subscriber: PubSubEventSubscriber[F],
    processor: BillTextProcessor[F],
    receivedEvent: ReceivedEvent,
    logger: PipelineLogger[F],
  ): F[ProcessingResult] = {
    val event         = receivedEvent.event.payload
    val correlationId = receivedEvent.event.correlationId
    processor.processEvent(event, correlationId).flatTap { result =>
      if (result.isSucceeded || result.isSkipped) {
        subscriber.acknowledge(List(receivedEvent.ackId))
      } else {
        logger.warn(
          repcheck.ingestion.common.logging.LogContext(
            runId = correlationId.toString,
            stepName = PipelineName,
          ),
          s"Not acking message for ${event.naturalKey} — processing failed, will be redelivered",
        )
      }
    }
  }

  private[app] def buildResources[F[_]: Async: Network](
    config: AppConfig,
    logger: PipelineLogger[F],
  ): Resource[F, PipelineResources[F]] =
    for {
      xa               <- TransactorResource.make[F](config.database)
      httpClient       <- EmberClientBuilder.default[F].build
      pubSubPublisher  <- PubSubPublisherResource.make[F](config.eventPublisher)
      pubSubSubscriber <- PubSubSubscriberResource.make[F](config.eventSubscriber, logger)
    } yield PipelineResources(xa, httpClient, pubSubPublisher, pubSubSubscriber)

}
