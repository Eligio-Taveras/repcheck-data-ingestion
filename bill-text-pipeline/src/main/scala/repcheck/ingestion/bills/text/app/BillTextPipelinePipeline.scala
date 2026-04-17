package repcheck.ingestion.bills.text.app

import cats.effect.{Async, ExitCode, Resource}
import cats.syntax.all._

import org.http4s.client.Client

import fs2.Stream

import doobie.util.transactor.Transactor

import repcheck.ingestion.bills.common.persistence.{DoobieBillRepository, DoobieBillTextVersionRepository}
import repcheck.ingestion.bills.text.config.BillTextPipelineConfig
import repcheck.ingestion.bills.text.download.BillTextDownloader
import repcheck.ingestion.bills.text.embedding.{EmbeddingConfig, OllamaEmbeddingService}
import repcheck.ingestion.bills.text.pipeline.BillTextProcessor
import repcheck.ingestion.bills.text.subscription.{EventSubscriberConfig, PubSubEventSubscriber, ReceivedEvent}
import repcheck.ingestion.common.db.DatabaseConfig
import repcheck.ingestion.common.events.{DefaultIngestionEventPublisher, EventPublisherConfig, PubSubEventPublisher}
import repcheck.ingestion.common.execution.{PipelineFailureHandlerConfig, WorkflowStateUpdater}
import repcheck.ingestion.common.logging.PipelineLogger
import repcheck.pipeline.models.errors.{RetryConfig, RetryWrapper}
import repcheck.pipeline.models.metadata.ProcessingResult

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
          // TODO: replace 0L with the Long run ID obtained from workflow_runs DB registration
          // once PipelineBootstrap.extractRunId (ingestion-common §3.7) is implemented.
          runId = 0L,
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
    val retryWrapper          = new RetryWrapper[F]((_, _, _, _, _, _) => Async[F].unit)
    val eventPublisher = new DefaultIngestionEventPublisher[F](
      publisher = pubSubPublisher,
      topicName = config.eventPublisher.topicName,
      source = config.eventPublisher.source,
      retryWrapper = retryWrapper,
      retryConfig = RetryConfig(),
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

  private[app] def buildResources[F[_]](
    config: AppConfig,
    logger: PipelineLogger[F],
    transactorFactory: DatabaseConfig => Resource[F, Transactor[F]],
    httpClientFactory: Resource[F, Client[F]],
    pubSubPublisherFactory: EventPublisherConfig => Resource[F, PubSubEventPublisher[F]],
    pubSubSubscriberFactory: (EventSubscriberConfig, PipelineLogger[F]) => Resource[F, PubSubEventSubscriber[F]],
  ): Resource[F, PipelineResources[F]] =
    for {
      xa               <- transactorFactory(config.database)
      httpClient       <- httpClientFactory
      pubSubPublisher  <- pubSubPublisherFactory(config.eventPublisher)
      pubSubSubscriber <- pubSubSubscriberFactory(config.eventSubscriber, logger)
    } yield PipelineResources(xa, httpClient, pubSubPublisher, pubSubSubscriber)

}
