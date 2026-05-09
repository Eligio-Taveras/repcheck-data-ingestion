package repcheck.ingestion.amendments.text.app

import java.util.concurrent.TimeoutException

import cats.effect.syntax.temporal._
import cats.effect.{Async, ExitCode, Resource}
import cats.syntax.all._

import org.http4s.client.Client

import fs2.Stream

import doobie.util.transactor.Transactor

import repcheck.ingestion.amendments.text.config.AmendmentTextPipelineConfig
import repcheck.ingestion.amendments.text.download.AmendmentTextDownloader
import repcheck.ingestion.amendments.text.embedding.CrossAmendmentEmbedder
import repcheck.ingestion.amendments.text.extraction.AmendmentTextExtractor
import repcheck.ingestion.amendments.text.persistence.{
  AmendmentTextChunkRepository,
  AmendmentTextVersionRepository,
  DoobieAmendmentTextChunkRepository,
  DoobieAmendmentTextVersionRepository,
}
import repcheck.ingestion.amendments.text.pipeline.AmendmentTextProcessor
import repcheck.ingestion.amendments.text.subscription.{EventSubscriberConfig, PubSubEventSubscriber, ReceivedEvent}
import repcheck.ingestion.common.db.DatabaseConfig
import repcheck.ingestion.common.execution.{PipelineFailureHandlerConfig, WorkflowStateUpdater}
import repcheck.ingestion.common.logging.PipelineLogger
import repcheck.ingestion.text.embedding.{EmbeddingConfig, OllamaEmbeddingService}
import repcheck.pipeline.models.metadata.ProcessingResult

/**
 * Testable wiring object for the amendment-text-pipeline. Pure logic; constructed by `runWithFactories` from
 * caller-supplied factory functions. Mirror of [[repcheck.ingestion.bills.text.app.BillTextPipelinePipeline]].
 */
private[app] object AmendmentTextPipelinePipeline {

  private val PipelineName = "amendment-text-pipeline"

  final case class AppConfig(
    database: DatabaseConfig,
    pipeline: AmendmentTextPipelineConfig,
    eventSubscriber: EventSubscriberConfig,
    embedding: EmbeddingConfig,
    failureHandler: PipelineFailureHandlerConfig,
  ) derives pureconfig.ConfigReader

  /**
   * Resource bundle. Two HTTP clients are intentional, mirroring the bill-side rationale:
   *
   *   - `govInfoClient` is rate-limited to keep the api.govinfo.gov budget under the published 36000/hour ceiling (see
   *     §7.6 spec, P7.1).
   *   - `ollamaClient` has NO rate limit. The local embedder sidecar has no external quota; sharing a rate-limit budget
   *     with govinfo caused a deadlock on the bill side (PR #83 follow-up) and we copy the fix.
   *
   * Note: no Pub/Sub publisher is wired here — §7.6 emits no completion events. Downstream consumers poll
   * `amendment_text_versions WHERE fetched_at IS NOT NULL`.
   */
  final case class PipelineResources[F[_]](
    xa: Transactor[F],
    govInfoClient: Client[F],
    ollamaClient: Client[F],
    pubSubSubscriber: PubSubEventSubscriber[F],
    embedder: CrossAmendmentEmbedder[F],
  )

  private[app] def runWithFactories[F[_]: Async](
    configLoader: F[AppConfig],
    loggerFactory: String => F[PipelineLogger[F]],
    resourceBuilder: (AppConfig, PipelineLogger[F]) => Resource[F, PipelineResources[F]],
    processorFactory: (
      Client[F],
      Transactor[F],
      CrossAmendmentEmbedder[F],
      AppConfig,
      PipelineLogger[F],
    ) => AmendmentTextProcessor[F],
    streamFactory: (
      PubSubEventSubscriber[F],
      AmendmentTextProcessor[F],
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
          resources.govInfoClient,
          resources.xa,
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
          // TODO: replace 0L with the Long run ID obtained from workflow_runs DB registration once
          // PipelineBootstrap.extractRunId (ingestion-common §3.7) is implemented.
          runId = 0L,
          workflowStateUpdater = workflowStateUpdater,
        )
      }
    } yield exitCode

  private[app] def buildProcessor[F[_]: Async](
    httpClient: Client[F],
    xa: Transactor[F],
    embedder: CrossAmendmentEmbedder[F],
    config: AppConfig,
    logger: PipelineLogger[F],
  ): AmendmentTextProcessor[F] = {
    val versionRepository: AmendmentTextVersionRepository[doobie.ConnectionIO] =
      new DoobieAmendmentTextVersionRepository
    val chunkRepository: AmendmentTextChunkRepository[doobie.ConnectionIO] =
      new DoobieAmendmentTextChunkRepository
    val downloader = new AmendmentTextDownloader[F](
      client = httpClient,
      govInfoApiKey = config.pipeline.govInfoApiKey,
      govInfoBaseUrl = config.pipeline.govInfoBaseUrl,
      logger = logger,
    )

    new AmendmentTextProcessor[F](
      downloader = downloader,
      amendmentTextVersionRepository = versionRepository,
      amendmentTextChunkRepository = chunkRepository,
      embedder = embedder,
      embeddingConfig = config.embedding,
      xa = xa,
      logger = logger,
      extractText = extractTextFn[F],
    )
  }

  // Extracted so unit tests can exercise the production extractText wiring without
  // needing to construct a full processor + drive processEvent.
  private[app] def extractTextFn[F[_]: Async]: (Stream[F, Byte], String) => Stream[F, String] =
    (bytes, format) => AmendmentTextExtractor.extractStream[F](bytes, format)

  /**
   * Builds the FS2 result stream from the Pub/Sub subscriber. Mirror of the bill-side `buildStream`. Each pull is
   * wrapped in `.timeout(pullTimeout)`; on `TimeoutException` we log a warning and short-circuit to an empty batch so
   * `takeWhile` cleanly terminates the stream — same defensive guard as the bill side.
   */
  private[app] def buildStream[F[_]: Async](
    subscriber: PubSubEventSubscriber[F],
    processor: AmendmentTextProcessor[F],
    config: AppConfig,
    logger: PipelineLogger[F],
  ): Stream[F, ProcessingResult] = {
    val pullTimeoutLogCtx = repcheck.ingestion.common.logging.LogContext(
      runId = "subscriber",
      stepName = "pubsub-pull",
    )
    val pullWithTimeout: F[List[ReceivedEvent]] =
      subscriber
        .pull(config.eventSubscriber.maxMessages)
        .timeout(config.eventSubscriber.pullTimeout)
        .recoverWith {
          case _: TimeoutException =>
            logger
              .warn(
                pullTimeoutLogCtx,
                s"Pub/Sub pull timed out after ${config.eventSubscriber.pullTimeout.toString}; " +
                  "treating as subscription drained for this run",
              )
              .as(List.empty[ReceivedEvent])
        }
    Stream
      .repeatEval(pullWithTimeout)
      .takeWhile(_.nonEmpty)
      .flatMap(Stream.emits)
      .parEvalMap(config.pipeline.parallelism) { receivedEvent =>
        processAndAck(subscriber, processor, receivedEvent, logger)
      }
  }

  private[app] def processAndAck[F[_]: Async](
    subscriber: PubSubEventSubscriber[F],
    processor: AmendmentTextProcessor[F],
    receivedEvent: ReceivedEvent,
    logger: PipelineLogger[F],
  ): F[ProcessingResult] = {
    val event = receivedEvent.event.payload
    processor.processEvent(event).flatTap { result =>
      if (result.isSucceeded || result.isSkipped) {
        subscriber.acknowledge(List(receivedEvent.ackId))
      } else {
        logger.warn(
          repcheck.ingestion.common.logging.LogContext(
            runId = event.correlationId.toString,
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
    govInfoClientFactory: Resource[F, Client[F]],
    ollamaClientFactory: Resource[F, Client[F]],
    pubSubSubscriberFactory: (EventSubscriberConfig, PipelineLogger[F]) => Resource[F, PubSubEventSubscriber[F]],
  ): Resource[F, PipelineResources[F]] =
    for {
      xa               <- transactorFactory(config.database)
      govInfoClient    <- govInfoClientFactory
      ollamaClient     <- ollamaClientFactory
      pubSubSubscriber <- pubSubSubscriberFactory(config.eventSubscriber, logger)
      embeddingService = new OllamaEmbeddingService[F](ollamaClient, config.embedding, logger)
      embedder <- CrossAmendmentEmbedder.resource[F](
        embeddingService = embeddingService,
        amendmentTextChunkRepository = new DoobieAmendmentTextChunkRepository,
        xa = xa,
        logger = logger,
        batchSize = config.embedding.embedBatchSize,
      )
    } yield PipelineResources(xa, govInfoClient, ollamaClient, pubSubSubscriber, embedder)

}
