package repcheck.ingestion.bills.text.app

import cats.effect.{Async, ExitCode, Resource}
import cats.syntax.all._

import org.http4s.client.Client

import fs2.Stream

import doobie.util.transactor.Transactor

import repcheck.ingestion.bills.common.persistence.{DoobieBillRepository, DoobieBillTextVersionRepository}
import repcheck.ingestion.bills.text.config.BillTextPipelineConfig
import repcheck.ingestion.bills.text.download.BillTextDownloader
import repcheck.ingestion.bills.text.embedding.{CrossBillEmbedder, EmbeddingConfig, OllamaEmbeddingService}
import repcheck.ingestion.bills.text.extraction.BillTextExtractor
import repcheck.ingestion.bills.text.persistence.DoobieRawBillTextRepository
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
    embedder: CrossBillEmbedder[F],
  )

  private[app] def runWithFactories[F[_]: Async](
    configLoader: F[AppConfig],
    loggerFactory: String => F[PipelineLogger[F]],
    resourceBuilder: (AppConfig, PipelineLogger[F]) => Resource[F, PipelineResources[F]],
    processorFactory: (
      Client[F],
      Transactor[F],
      PubSubEventPublisher[F],
      CrossBillEmbedder[F],
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
          resources.embedder,
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
    embedder: CrossBillEmbedder[F],
    config: AppConfig,
    logger: PipelineLogger[F],
  ): BillTextProcessor[F] = {
    val billRepository        = new DoobieBillRepository
    val textVersionRepository = new DoobieBillTextVersionRepository
    val rawBillTextRepository = new DoobieRawBillTextRepository
    val downloader            = new BillTextDownloader[F](httpClient, logger)
    val retryWrapper          = new RetryWrapper[F]((_, _, _, _, _, _) => Async[F].unit)
    val eventPublisher = new DefaultIngestionEventPublisher[F](
      publisher = pubSubPublisher,
      topicName = config.eventPublisher.topicName,
      source = config.eventPublisher.source,
      retryWrapper = retryWrapper,
      retryConfig = RetryConfig(),
    )

    new BillTextProcessor[F](
      downloader = downloader,
      billRepository = billRepository,
      textVersionRepository = textVersionRepository,
      rawBillTextRepository = rawBillTextRepository,
      embedder = embedder,
      embeddingConfig = config.embedding,
      eventPublisher = eventPublisher,
      xa = xa,
      logger = logger,
      extractText = (bytes, format) => BillTextExtractor.extractStream[F](bytes, format),
    )
  }

  /**
   * Builds the FS2 result stream from the Pub/Sub subscriber. Pulls batches of messages until the subscription drains,
   * deserializes each into a `PipelineEvent[BillTextAvailableEvent]`, processes via the `BillTextProcessor`, and
   * acknowledges successfully processed messages. The correlationId is extracted from each `PipelineEvent` envelope —
   * one correlationId per bill.
   *
   * `repeatEval` + `takeWhile(_.nonEmpty)` keeps issuing Pull RPCs until one returns zero messages, then terminates the
   * stream. This works around the Pub/Sub emulator's per-RPC cap (100 messages regardless of `maxMessages` requested)
   * without changing observed behavior on production GCP Pub/Sub: there too we keep pulling as long as messages are
   * available, only stopping when the subscription is empty. Backpressure flows through `parEvalMap` — pulls happen on
   * demand as parallelism slots free up, so the next RPC fires before the previous batch's last bill finishes
   * processing.
   */
  private[app] def buildStream[F[_]: Async](
    subscriber: PubSubEventSubscriber[F],
    processor: BillTextProcessor[F],
    config: AppConfig,
    logger: PipelineLogger[F],
  ): Stream[F, ProcessingResult] =
    Stream
      .repeatEval(subscriber.pull(config.eventSubscriber.maxMessages))
      .takeWhile(_.nonEmpty)
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

  private[app] def buildResources[F[_]: Async](
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
      embeddingService = new OllamaEmbeddingService[F](httpClient, config.embedding, logger)
      embedder <- CrossBillEmbedder.resource[F](
        embeddingService = embeddingService,
        rawBillTextRepository = new DoobieRawBillTextRepository,
        xa = xa,
        logger = logger,
        embedBatchSize = config.embedding.embedBatchSize,
        embedBatchTimeout = config.embedding.embedBatchTimeout,
        queueCapacity = config.embedding.embedBatchSize * config.embedding.embedQueueCapacityMultiplier,
      )
    } yield PipelineResources(xa, httpClient, pubSubPublisher, pubSubSubscriber, embedder)

}
