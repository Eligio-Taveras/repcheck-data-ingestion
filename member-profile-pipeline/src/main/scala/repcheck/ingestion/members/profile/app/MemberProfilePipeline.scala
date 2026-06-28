package repcheck.ingestion.members.profile.app

import java.util.UUID

import cats.effect.{Async, ExitCode, Resource}
import cats.syntax.all._

import org.http4s.client.Client

import fs2.Stream

import doobie.util.transactor.Transactor

import repcheck.ingestion.common.api.CongressGovClientConfig
import repcheck.ingestion.common.db.DatabaseConfig
import repcheck.ingestion.common.events.{DefaultIngestionEventPublisher, EventPublisherConfig, PubSubEventPublisher}
import repcheck.ingestion.common.execution.PipelineExecutor
import repcheck.ingestion.common.ids.{RunId, StepRunId}
import repcheck.ingestion.common.logging.PipelineLogger
import repcheck.ingestion.members.profile.api.MembersApiClient
import repcheck.ingestion.members.profile.config.MemberProfileConfig
import repcheck.ingestion.members.profile.pipeline.MemberProfileProcessor
import repcheck.members.common.persistence.{
  DoobieMemberHistoryArchiver,
  DoobieMemberPartyHistoryRepository,
  DoobieMemberRepository,
  DoobieMemberTermRepository,
}
import repcheck.pipeline.models.metadata.ProcessingResult

import com.repcheck.utils.errors.{ErrorClass, RetryWrapper}

/**
 * Testable wiring for the member profile pipeline. All resource acquisition and object construction is factored out of
 * [[MemberProfilePipelineApp]] so that tests can inject stubs for any dependency without wiring real infrastructure.
 */
private[app] object MemberProfilePipeline {

  private val PipelineName = "member-profile-pipeline"

  final case class AppConfig(
    database: DatabaseConfig,
    congressApi: CongressGovClientConfig,
    pipeline: MemberProfileConfig,
    eventPublisher: EventPublisherConfig,
  ) derives pureconfig.ConfigReader

  /** Resource bundle produced by [[buildResources]] — groups all managed dependencies. */
  final case class PipelineResources[F[_]](
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
   *   constructs a [[MemberProfileProcessor]] from resolved dependencies
   * @param congressesResolver
   *   resolves the list of congresses to ingest (env > config > DB-derived). Stubbed in tests.
   * @param streamFactory
   *   builds the [[Stream]] from the processor + resolved congresses (allows tests to inject a canned result stream)
   */
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
    ) => MemberProfileProcessor[F],
    congressesResolver: (AppConfig, Transactor[F], PipelineLogger[F]) => F[List[Int]],
    streamFactory: (MemberProfileProcessor[F], PipelineLogger[F], List[Int], RunId) => Stream[F, ProcessingResult],
    runId: RunId,
    stepRunId: StepRunId,
  ): F[ExitCode] =
    for {
      config <- configLoader
      logger <- loggerFactory(PipelineName)
      exitCode <- resourceBuilder(config, logger).use { resources =>
        for {
          congresses <- congressesResolver(config, resources.xa, logger)
          processor    = processorFactory(resources.httpClient, resources.xa, resources.pubSubPublisher, config, logger)
          resultStream = streamFactory(processor, logger, congresses, runId)
          result <- PipelineExecutor.execute[F](resultStream, logger, PipelineName, runId, stepRunId)
        } yield result
      }
    } yield exitCode

  /**
   * No-op retry logger used when we don't need per-attempt logging. Extracted as a named method so tests can exercise
   * its body directly — an inlined lambda is never executed unless a retry fires.
   */
  private[app] def noOpRetryLogger[F[_]: Async]: (Int, Int, Long, ErrorClass, String, UUID) => F[Unit] =
    (_, _, _, _, _, _) => Async[F].unit

  /**
   * Constructs a [[MemberProfileProcessor]] from resolved low-level dependencies. All repos are plain no-arg classes;
   * the retry wrapper and event publisher are wired here using config values.
   */
  private[app] def buildProcessor[F[_]: Async](
    httpClient: Client[F],
    xa: Transactor[F],
    pubSubPublisher: PubSubEventPublisher[F],
    config: AppConfig,
    logger: PipelineLogger[F],
  ): MemberProfileProcessor[F] = {
    val memberRepo       = new DoobieMemberRepository
    val termRepo         = new DoobieMemberTermRepository
    val partyHistoryRepo = new DoobieMemberPartyHistoryRepository
    val historyArchiver  = new DoobieMemberHistoryArchiver
    val retryWrapper     = new RetryWrapper[F](noOpRetryLogger[F])
    val eventPublisher = new DefaultIngestionEventPublisher[F](
      publisher = pubSubPublisher,
      topicName = config.eventPublisher.topicName,
      source = config.eventPublisher.source,
      retryWrapper = retryWrapper,
      retryConfig = config.pipeline.eventPublishRetry,
    )
    val apiClient = MembersApiClient[F](config.congressApi, httpClient, retryWrapper)

    new MemberProfileProcessor[F](
      apiClient = apiClient,
      memberRepo = memberRepo,
      termRepo = termRepo,
      partyHistoryRepo = partyHistoryRepo,
      historyArchiver = historyArchiver,
      eventPublisher = eventPublisher,
      xa = xa,
      config = config.pipeline,
      logger = logger,
    )
  }

  /**
   * Builds the result stream by delegating to `processor.streamAll`. A fresh correlation ID is generated for this
   * pipeline run.
   */
  private[app] def buildStream[F[_]](
    processor: MemberProfileProcessor[F],
    logger: PipelineLogger[F],
    congresses: List[Int],
    runId: RunId,
  ): Stream[F, ProcessingResult] = {
    val _ = logger // reserved for future pre/post-stream logging
    processor.streamAll(runId.value, congresses)
  }

  /**
   * Acquires all managed resources (transactor, HTTP client, Pub/Sub publisher) using the provided factory functions.
   * Supports dependency injection in tests.
   */
  private[app] def buildResources[F[_]](
    config: AppConfig,
    logger: PipelineLogger[F],
    transactorFactory: DatabaseConfig => Resource[F, Transactor[F]],
    httpClientFactory: Resource[F, Client[F]],
    pubSubPublisherFactory: EventPublisherConfig => Resource[F, PubSubEventPublisher[F]],
  ): Resource[F, PipelineResources[F]] = {
    val _ = logger // reserved for future resource-level logging
    for {
      xa              <- transactorFactory(config.database)
      httpClient      <- httpClientFactory
      pubSubPublisher <- pubSubPublisherFactory(config.eventPublisher)
    } yield PipelineResources(xa, httpClient, pubSubPublisher)
  }

}
