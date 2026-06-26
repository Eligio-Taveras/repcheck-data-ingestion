package repcheck.members.lismapping.app

import java.util.UUID

import cats.effect.{Async, ExitCode, Resource}
import cats.syntax.all._

import org.http4s.client.Client

import doobie.util.transactor.Transactor

import repcheck.ingestion.common.db.DatabaseConfig
import repcheck.ingestion.common.events.{DefaultIngestionEventPublisher, EventPublisherConfig, PubSubEventPublisher}
import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}
import repcheck.ingestion.common.xml.XmlFeedClient
import repcheck.members.common.persistence.{
  DoobieLisMappingRepository,
  DoobieLisMemberRepository,
  DoobieMemberRepository,
}
import repcheck.members.lismapping.client.SenatorLookupXmlClient
import repcheck.members.lismapping.config.LisMappingConfig
import repcheck.members.lismapping.pipeline.LisMappingProcessor

import com.repcheck.utils.errors.{ErrorClass, RetryConfig, RetryWrapper}

/**
 * Testable wiring for the LIS mapping refresher. All resource acquisition and object construction is extracted from
 * [[LisMappingRefresherApp]] so that tests can inject stubs without wiring real infrastructure.
 *
 * Unlike the bill/member profile pipelines, the refresher does not produce a `Stream[F, ProcessingResult]` — it returns
 * an [[LisMappingRefreshResult]] summary for the entire run. The exit code is always `ExitCode.Success` if
 * [[LisMappingProcessor.refreshAll]] completes; any error propagates as a raised effect and the IOApp runtime converts
 * it to a non-zero exit.
 */
private[app] object LisMappingRefresherPipeline {

  private val PipelineName = "lis-mapping-refresher"

  final case class AppConfig(
    database: DatabaseConfig,
    pipeline: LisMappingConfig,
    fetchRetry: RetryConfig,
    eventPublisher: EventPublisherConfig,
  ) derives pureconfig.ConfigReader

  /** Resource bundle produced by [[buildResources]] — groups all managed dependencies. */
  final case class RefresherResources[F[_]](
    xa: Transactor[F],
    httpClient: Client[F],
    pubSubPublisher: PubSubEventPublisher[F],
  )

  /**
   * Main entry point with full factory injection. The IOApp passes real factory implementations; tests pass stubs.
   *
   * @param configLoader
   *   effect that loads [[AppConfig]] (production: PureConfig; tests: `IO.pure(testConfig)`)
   * @param loggerFactory
   *   creates a [[PipelineLogger]] for the given pipeline name
   * @param resourceBuilder
   *   acquires the transactor, HTTP client, and Pub/Sub publisher as a [[Resource]]
   * @param processorFactory
   *   constructs a [[LisMappingProcessor]] from resolved dependencies
   */
  private[app] def runWithFactories[F[_]: Async](
    configLoader: F[AppConfig],
    loggerFactory: String => F[PipelineLogger[F]],
    resourceBuilder: (AppConfig, PipelineLogger[F]) => Resource[F, RefresherResources[F]],
    processorFactory: (
      Client[F],
      Transactor[F],
      PubSubEventPublisher[F],
      AppConfig,
      PipelineLogger[F],
    ) => LisMappingProcessor[F],
    runId: Long = 0L,
  ): F[ExitCode] =
    for {
      config <- configLoader
      logger <- loggerFactory(PipelineName)
      exitCode <- resourceBuilder(config, logger).use { resources =>
        val processor = processorFactory(resources.httpClient, resources.xa, resources.pubSubPublisher, config, logger)
        val logCtx    = LogContext(runId = runId.toString, stepName = PipelineName)
        for {
          result <- processor.refreshAll(runId)
          _ <- logger.info(
            logCtx,
            s"Pipeline completed: totalParsed=${result.totalParsed.toString} " +
              s"inserted=${result.inserted.toString} updated=${result.updated.toString} " +
              s"eventsEmitted=${result.eventsEmitted.toString}",
          )
        } yield ExitCode.Success
      }
    } yield exitCode

  /**
   * No-op retry logger. Extracted as a named method so tests can exercise its body — inlined lambdas are never reached
   * when no retries fire in unit tests.
   */
  private[app] def noOpRetryLogger[F[_]: Async]: (Int, Int, Long, ErrorClass, String, UUID) => F[Unit] =
    (_, _, _, _, _, _) => Async[F].unit

  /**
   * Constructs a [[LisMappingProcessor]] from resolved low-level dependencies. Wires [[XmlFeedClient]],
   * [[SenatorLookupXmlClient]], all repositories, and the event publisher.
   */
  private[app] def buildProcessor[F[_]: Async](
    httpClient: Client[F],
    xa: Transactor[F],
    pubSubPublisher: PubSubEventPublisher[F],
    config: AppConfig,
    logger: PipelineLogger[F],
  ): LisMappingProcessor[F] = {
    given org.typelevel.log4cats.Logger[F] =
      org.typelevel.log4cats.slf4j.Slf4jLogger.getLoggerFromName[F](PipelineName)
    val retryWrapper  = new RetryWrapper[F](noOpRetryLogger[F])
    val xmlFeedClient = XmlFeedClient.make[F](httpClient, config.fetchRetry)
    val xmlClient     = new SenatorLookupXmlClient[F](xmlFeedClient, config.pipeline, logger)
    val lisMemberRepo = new DoobieLisMemberRepository
    val mappingRepo   = new DoobieLisMappingRepository
    val memberRepo    = new DoobieMemberRepository
    val eventPublisher = new DefaultIngestionEventPublisher[F](
      publisher = pubSubPublisher,
      topicName = config.eventPublisher.topicName,
      source = config.eventPublisher.source,
      retryWrapper = retryWrapper,
      retryConfig = config.fetchRetry,
    )

    new LisMappingProcessor[F](
      xmlClient = xmlClient,
      lisMemberRepo = lisMemberRepo,
      mappingRepo = mappingRepo,
      memberRepo = memberRepo,
      eventPublisher = eventPublisher,
      xa = xa,
      config = config.pipeline,
      logger = logger,
    )
  }

  /**
   * Acquires all managed resources using the provided factory functions. Supports dependency injection in tests.
   */
  private[app] def buildResources[F[_]](
    config: AppConfig,
    logger: PipelineLogger[F],
    transactorFactory: DatabaseConfig => Resource[F, Transactor[F]],
    httpClientFactory: Resource[F, Client[F]],
    pubSubPublisherFactory: EventPublisherConfig => Resource[F, PubSubEventPublisher[F]],
  ): Resource[F, RefresherResources[F]] = {
    val _ = logger // reserved for future resource-level logging
    for {
      xa              <- transactorFactory(config.database)
      httpClient      <- httpClientFactory
      pubSubPublisher <- pubSubPublisherFactory(config.eventPublisher)
    } yield RefresherResources(xa, httpClient, pubSubPublisher)
  }

}
