package repcheck.ingestion.amendments.text.app

import java.util.concurrent.TimeoutException

import cats.effect.syntax.temporal._
import cats.effect.std.UUIDGen
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
  AmendmentTextVersionRepository,
  DoobieAmendmentTextChunkRepository,
  DoobieAmendmentTextVersionRepository,
}
import repcheck.ingestion.amendments.text.pipeline.AmendmentTextProcessor
import repcheck.ingestion.amendments.text.subscription.{EventSubscriberConfig, PubSubEventSubscriber, ReceivedEvent}
import repcheck.ingestion.common.db.DatabaseConfig
import repcheck.ingestion.common.execution.{PipelineExecutor, PipelineFailureHandlerConfig, WorkflowStateUpdater}
import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}
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
    transactor: Transactor[F],
    govInfoClient: Client[F],
    ollamaClient: Client[F],
    pubSubSubscriber: PubSubEventSubscriber[F],
    embedder: CrossAmendmentEmbedder[F],
  )

  /**
   * @param runId
   *   workflow-run identifier passed from the IOApp's CLI args. `0L` is the placeholder used when this Cloud Run
   *   Service runs without a workflow registrar — threaded into LogContext (`runId.toString`) and
   *   `PipelineExecutor.execute`.
   * @param stepRunId
   *   workflow_run_steps row identifier from the IOApp's CLI args. Stored on the [[StepRunSummary]] as the foreign key
   *   into `workflow_run_steps`. Defaults to `0L` — placeholder until that table is wired up.
   */
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
      Long,
    ) => Stream[F, ProcessingResult],
    workflowStateUpdaterFactory: (Transactor[F], PipelineFailureHandlerConfig) => Option[WorkflowStateUpdater[F]],
    runId: Long = 0L,
    stepRunId: Long = 0L,
  ): F[ExitCode] =
    for {
      config <- configLoader
      logger <- loggerFactory(PipelineName)
      exitCode <- resourceBuilder(config, logger).use { resources =>
        val processor = processorFactory(
          resources.govInfoClient,
          resources.transactor,
          resources.embedder,
          config,
          logger,
        )
        val workflowStateUpdater = workflowStateUpdaterFactory(resources.transactor, config.failureHandler)
        val resultStream         = streamFactory(resources.pubSubSubscriber, processor, config, logger, runId)
        PipelineExecutor.execute[F](
          resultStream = resultStream,
          logger = logger,
          pipelineName = PipelineName,
          runId = runId.toString,
          stepRunId = stepRunId,
          workflowStateUpdater = workflowStateUpdater,
        )
      }
    } yield exitCode

  private[app] def buildProcessor[F[_]: Async](
    httpClient: Client[F],
    transactor: Transactor[F],
    embedder: CrossAmendmentEmbedder[F],
    config: AppConfig,
    logger: PipelineLogger[F],
  ): AmendmentTextProcessor[F] = {
    val versionRepository: AmendmentTextVersionRepository[doobie.ConnectionIO] =
      new DoobieAmendmentTextVersionRepository
    val downloader = new AmendmentTextDownloader[F](
      client = httpClient,
      govInfoApiKey = config.pipeline.govInfoApiKey,
      govInfoBaseUrl = config.pipeline.govInfoBaseUrl,
      logger = logger,
    )

    new AmendmentTextProcessor[F](
      downloader = downloader,
      amendmentTextVersionRepository = versionRepository,
      embedder = embedder,
      embeddingConfig = config.embedding,
      xa = transactor,
      logger = logger,
      extractText = extractTextFn[F],
    )
  }

  // Extracted so unit tests can exercise the production extractText wiring without
  // needing to construct a full processor + drive processEvent.
  private[app] def extractTextFn[F[_]: Async]: (Stream[F, Byte], String, String) => Stream[F, String] =
    (bytes, format, naturalKey) => AmendmentTextExtractor.extractStream[F](bytes, format, naturalKey)

  /**
   * Builds the FS2 result stream from the Pub/Sub subscriber. Mirror of the bill-side `buildStream`. Each pull is
   * wrapped in `.timeout(pullTimeout)`; on `TimeoutException` we log a warning and short-circuit to an empty batch so
   * `takeWhile` cleanly terminates the stream — same defensive guard as the bill side.
   *
   * `runId` is threaded into the pull-timeout LogContext so a stuck pull's warning is grouped with the rest of this
   * run's lines in log aggregation.
   */
  private[app] def buildStream[F[_]: Async](
    subscriber: PubSubEventSubscriber[F],
    processor: AmendmentTextProcessor[F],
    config: AppConfig,
    logger: PipelineLogger[F],
    runId: Long,
  ): Stream[F, ProcessingResult] = {
    val pullTimeoutLogCtx = LogContext(
      runId = runId.toString,
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
        processWithDelegatedAck(subscriber, processor, receivedEvent)
      }
  }

  /**
   * Wire per-message `ack` / `nack` effects to the processor. The processor either:
   *   - hands off the chunk stream to the embedder (which ACKs when chunks land), or
   *   - fires `ack` synchronously for `Skipped` (already-ingested), or
   *   - fires `nack` synchronously when the version-row UPSERT raises (Failed).
   *
   * `ProcessingResult` here is a stats signal only — the actual Pub/Sub ACK lifecycle is owned by either the processor
   * (skip / version-upsert failure) or the embedder (chunk persistence).
   */
  private[app] def processWithDelegatedAck[F[_]](
    subscriber: PubSubEventSubscriber[F],
    processor: AmendmentTextProcessor[F],
    receivedEvent: ReceivedEvent,
  ): F[ProcessingResult] = {
    val event = receivedEvent.event.payload
    val ackId = receivedEvent.ackId
    val ack   = subscriber.acknowledge(List(ackId))
    val nack  = subscriber.nack(List(ackId))
    processor.processEvent(event, ackId, ack, nack)
  }

  private[app] def buildResources[F[_]: Async: UUIDGen](
    config: AppConfig,
    logger: PipelineLogger[F],
    transactorFactory: DatabaseConfig => Resource[F, Transactor[F]],
    govInfoClientFactory: Resource[F, Client[F]],
    ollamaClientFactory: Resource[F, Client[F]],
    pubSubSubscriberFactory: (EventSubscriberConfig, PipelineLogger[F]) => Resource[F, PubSubEventSubscriber[F]],
  ): Resource[F, PipelineResources[F]] =
    for {
      transactor       <- transactorFactory(config.database)
      govInfoClient    <- govInfoClientFactory
      ollamaClient     <- ollamaClientFactory
      pubSubSubscriber <- pubSubSubscriberFactory(config.eventSubscriber, logger)
      embeddingService = new OllamaEmbeddingService[F](ollamaClient, config.embedding, logger)
      embedder <- CrossAmendmentEmbedder.resource[F](
        embeddingService = embeddingService,
        amendmentTextChunkRepository = new DoobieAmendmentTextChunkRepository,
        amendmentTextVersionRepository = new DoobieAmendmentTextVersionRepository,
        xa = transactor,
        logger = logger,
        batchSize = config.embedding.embedBatchSize,
      )
    } yield PipelineResources(transactor, govInfoClient, ollamaClient, pubSubSubscriber, embedder)

}
