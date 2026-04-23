package repcheck.ingestion.votes.app

import scala.concurrent.duration.FiniteDuration

import cats.effect.std.Semaphore
import cats.effect.{Async, ExitCode, Resource, Sync, Temporal}
import cats.syntax.all._

import org.http4s.client.Client
import org.http4s.ember.client.EmberClientBuilder

import fs2.Stream
import fs2.io.net.Network

import doobie.implicits._
import doobie.util.transactor.Transactor

import pureconfig.ConfigSource

import repcheck.ingestion.bills.common.persistence.DoobieBillRepository
import repcheck.ingestion.common.api.CongressGovClientConfig
import repcheck.ingestion.common.db.{DatabaseConfig, TransactorResource}
import repcheck.ingestion.common.events.{
  DefaultIngestionEventPublisher,
  EventPublisherConfig,
  IngestionEventPublisher,
  PubSubEventPublisher,
  PubSubPublisherResource,
}
import repcheck.ingestion.common.execution.PipelineBootstrap
import repcheck.ingestion.common.logging.{PipelineLogger, PipelineLoggerFactory}
import repcheck.ingestion.common.placeholders.{DefaultPlaceholderCreator, DoobieEntityRepository}
import repcheck.ingestion.votes.api.HouseVotesApiClient
import repcheck.ingestion.votes.config.VotesPipelineConfig
import repcheck.ingestion.votes.errors.StepRunIdInvalid
import repcheck.ingestion.votes.lis.LisResolver
import repcheck.ingestion.votes.pipeline._
import repcheck.ingestion.votes.repo.{
  DoobieStanceMaterializationStatusRepository,
  DoobieVoteHistoryArchiver,
  DoobieVotePositionRepository,
  DoobieVoteRepository,
}
import repcheck.ingestion.votes.xml.SenateVoteXmlClient
import repcheck.members.common.MemberInsertSql
import repcheck.members.common.persistence.{DoobieLisMemberRepository, DoobieMemberRepository, MemberWriteInstances}
import repcheck.pipeline.models.errors.{RetryConfig, RetryWrapper}
import repcheck.pipeline.models.metadata.ProcessingResult
import repcheck.shared.models.congress.dos.member.MemberDO
import repcheck.shared.models.congress.dos.vote.{VoteDO, VotePositionDO}

/**
 * Testable wiring for the votes pipeline. Composition lives in a companion object so tests can exercise the `run`
 * machinery with stubbed factories while the production `IOApp` stays a trivial delegation.
 *
 * ==Shape==
 *
 *   - [[run]] — production entry point. Builds real factories and calls [[runWithFactories]].
 *   - [[runWithFactories]] — accepts factory functions for every side-effecting collaborator. Tests inject stubs.
 *   - [[buildResources]] — composes managed resources (transactor, two rate-limited HTTP clients, Pub/Sub publisher)
 *     into a [[Resources]] bundle. Accepts raw factories so tests can swap the underlying Ember/pubsub SDK out.
 *   - [[buildProcessor]] — constructs the full [[VoteProcessor]] graph from resources + config. Pure function over a
 *     provided [[Resources]] — tests can pass a bundle backed by mocks.
 *
 * ==Per-chamber HTTP pacing==
 *
 * Both the House (Congress.gov JSON) and Senate (senate.gov XML) clients share a single underlying
 * `EmberClientBuilder.default` resource, but each wraps it through [[rateLimitedClient]] with its own configurable
 * delay (`house.pageDelay` vs. `senate.requestDelay`). The wrapper uses a one-permit `Semaphore` to serialize requests
 * on each wrapped client so per-request pacing is respected even when upstream `parEvalMap(parallelism > 1)` submits
 * calls concurrently.
 *
 * ==Launcher contract==
 *
 *   - `args(0)` — config JSON (consumed via [[PipelineBootstrap.loadConfig]] or the embedded `application.conf`).
 *   - `args(1)` — run-level identifier (`workflow_runs.id` string).
 *   - `args(2)` — step-level identifier (`workflow_run_steps.id` Long).
 */
private[app] object VotesPipeline {

  private val PipelineName = "votes-pipeline"

  final case class AppConfig(
    database: DatabaseConfig,
    congressApi: CongressGovClientConfig,
    pipeline: VotesPipelineConfig,
    eventPublisher: EventPublisherConfig,
  ) derives pureconfig.ConfigReader

  /**
   * Resource bundle produced by [[buildResources]]. Tests can construct this directly with mocks for each field to
   * bypass the Resource acquisition machinery entirely.
   */
  final case class Resources[F[_]](
    xa: Transactor[F],
    houseClient: Client[F],
    senateClient: Client[F],
    eventPublisher: IngestionEventPublisher[F],
  )

  /**
   * Production entry point. Wires real factories and delegates to [[runWithFactories]]. Keep this method as the ONLY
   * place in the codebase that constructs the live GCP / Congress.gov / AlloyDB SDK resources — everything downstream
   * is testable by swapping factories.
   */
  def run[F[_]: Async: Network](args: List[String]): F[ExitCode] =
    runWithFactories[F](
      args = args,
      configLoader = Sync[F].delay(ConfigSource.default.loadOrThrow[AppConfig]),
      loggerFactory = PipelineLoggerFactory.make[F](PipelineName),
      resourceBuilder = (cfg: AppConfig) =>
        buildResources[F](
          config = cfg,
          transactorFactory = TransactorResource.make[F](_),
          httpClientFactory = EmberClientBuilder.default[F].build,
          pubSubPublisherFactory = PubSubPublisherResource.make[F](_),
        ),
      processorFactory = buildProcessor[F],
      streamFactory = (processor: VoteProcessor[F], runId: String) => processor.streamAll(runId),
    )

  /**
   * Testable runtime. Every collaborator that performs a side effect at app startup is supplied via a factory function.
   * The unit spec uses this to verify ordering (`configLoader` runs once, then `loggerFactory`, then `resourceBuilder`,
   * then `processorFactory`, then `streamFactory`) without constructing any real dependency.
   */
  private[app] def runWithFactories[F[_]: Async](
    args: List[String],
    configLoader: F[AppConfig],
    loggerFactory: F[PipelineLogger[F]],
    resourceBuilder: AppConfig => Resource[F, Resources[F]],
    processorFactory: (AppConfig, Resources[F], PipelineLogger[F]) => VoteProcessor[F],
    streamFactory: (VoteProcessor[F], String) => Stream[F, ProcessingResult],
  ): F[ExitCode] =
    for {
      config    <- configLoader
      runId     <- PipelineBootstrap.extractRunId[F](args)
      stepRunId <- extractStepRunId[F](args)
      logger    <- loggerFactory
      exitCode <- resourceBuilder(config).use { resources =>
        val processor = processorFactory(config, resources, logger)
        val stream    = streamFactory(processor, runId)
        PipelineExecutor.execute[F](stream, logger, PipelineName, runId, stepRunId)
      }
    } yield exitCode

  /**
   * Compose the managed resources needed by [[buildProcessor]]:
   *   - a Doobie `Transactor[F]` against AlloyDB / Cloud SQL PostgreSQL;
   *   - a `Client[F]` per chamber, each pre-wrapped with its own [[rateLimitedClient]] and pacing delay;
   *   - a Google Pub/Sub `PubSubEventPublisher[F]`, wrapped as the higher-level `IngestionEventPublisher[F]` so
   *     [[VoteEventEmitter]] only sees the application-facing API.
   *
   * Accepts the low-level factories as parameters so tests can substitute fixed values without pulling in real GCP /
   * AlloyDB / HTTP libraries.
   */
  private[app] def buildResources[F[_]: Async](
    config: AppConfig,
    transactorFactory: DatabaseConfig => Resource[F, Transactor[F]],
    httpClientFactory: Resource[F, Client[F]],
    pubSubPublisherFactory: EventPublisherConfig => Resource[F, PubSubEventPublisher[F]],
  ): Resource[F, Resources[F]] =
    for {
      xa              <- transactorFactory(config.database)
      rawClient       <- httpClientFactory
      houseClient     <- rateLimitedClient(rawClient, config.pipeline.house.pageDelay)
      senateClient    <- rateLimitedClient(rawClient, config.pipeline.senate.requestDelay)
      pubSubPublisher <- pubSubPublisherFactory(config.eventPublisher)
      retryWrapper = new RetryWrapper[F]((_, _, _, _, _, _) => Async[F].unit)
      publisher = new DefaultIngestionEventPublisher[F](
        publisher = pubSubPublisher,
        topicName = config.eventPublisher.topicName,
        source = config.eventPublisher.source,
        retryWrapper = retryWrapper,
        retryConfig = RetryConfig(),
      )
    } yield Resources(xa, houseClient, senateClient, publisher)

  /**
   * Construct the full [[VoteProcessor]] dependency graph from a [[Resources]] bundle plus config and logger. Pure
   * function — no IO — so tests can construct processors deterministically given a resource stub.
   *
   * Every repository and collaborator is instantiated here. `MemberWriteInstances._` is imported locally so the
   * implicit `Write[MemberDO]` can resolve when `DoobieEntityRepository[F, MemberDO]` is constructed below. Bill
   * placeholders use the votes-local [[DoobieBillPlaceholderRepository]] because `bills` has no `natural_key` column
   * and the shared `HasPlaceholder[BillDO]` alone can't produce unique composite keys.
   */
  private[app] def buildProcessor[F[_]: Async](
    config: AppConfig,
    resources: Resources[F],
    logger: PipelineLogger[F],
  ): VoteProcessor[F] = {
    import MemberWriteInstances._ // provides Write[MemberDO] for DoobieEntityRepository

    // Zero-arg Doobie repositories
    val voteRepo      = new DoobieVoteRepository
    val positionRepo  = new DoobieVotePositionRepository
    val historyRepo   = new DoobieVoteHistoryArchiver
    val stanceRepo    = new DoobieStanceMaterializationStatusRepository
    val memberRepo    = new DoobieMemberRepository
    val lisMemberRepo = new DoobieLisMemberRepository
    val billRepo      = new DoobieBillRepository

    // Placeholder machinery. Members use the generic DoobieEntityRepository + MemberInsertSql; bills use the
    // votes-pipeline-local placeholder repo that parses the composite natural key into congress/bill_type/number.
    val placeholderCreator = new DefaultPlaceholderCreator[F]
    val memberEntityRepo   = new DoobieEntityRepository[F, MemberDO](resources.xa, MemberInsertSql.value)
    val billEntityRepo     = new DoobieBillPlaceholderRepository[F](resources.xa)

    // Single no-op RetryWrapper shared across API clients. The wrapper's logging callback is deliberately empty —
    // per-retry logs come from the structured PipelineLogger at each client's boundary instead.
    val retryWrapper = new RetryWrapper[F]((_, _, _, _, _, _) => Async[F].unit)

    // API clients. Each receives a pre-rate-limited Client[F] from `resources` — no client re-wraps internally.
    val houseApiClient = new HouseVotesApiClient[F](
      config = config.congressApi,
      houseConfig = config.pipeline.house,
      client = resources.houseClient,
      retryWrapper = retryWrapper,
      temporalInstance = Temporal[F],
    )
    val senateXmlClient = new SenateVoteXmlClient[F](
      httpClient = resources.senateClient,
      retryWrapper = retryWrapper,
      config = config.pipeline.senate,
      logger = logger,
    )

    // Processor collaborators — every one is a public class (see access-widening in P3.2 for the decomposed components).
    val lisResolver = LisResolver[F](lisMemberRepo, resources.xa, logger)
    val billLookup = new BillLookup[F](
      billRepo = billRepo,
      billEntityRepo = billEntityRepo,
      placeholderCreator = placeholderCreator,
      xa = resources.xa,
      logger = logger,
    )
    val memberLookup = new MemberLookup[F](
      memberRepo = memberRepo,
      memberEntityRepo = memberEntityRepo,
      placeholderCreator = placeholderCreator,
      xa = resources.xa,
      logger = logger,
    )
    val houseConverter  = new HouseVoteConverter[F](logger)
    val senateConverter = new SenateVoteConverter[F](logger, config.pipeline.senate.baseUrl)
    val persister = new VotePersister[F](
      voteRepo = voteRepo,
      positionRepo = positionRepo,
      historyArchiver = historyRepo,
      xa = resources.xa,
    )
    val eventEmitter = new VoteEventEmitter[F](
      stanceRepo = stanceRepo,
      eventPublisher = resources.eventPublisher,
      xa = resources.xa,
      logger = logger,
    )

    // Stored-state callbacks for VoteChangeDetector and VoteProcessor. Both wrap the Doobie read as a transaction
    // against the shared transactor so detector logic stays unit-testable (it receives an `F[...]`, not a ConnectionIO).
    val findStoredVote: String => F[Option[VoteDO]] =
      nk => voteRepo.findByNaturalKey(nk).transact(resources.xa)
    val findStoredPositions: Long => F[List[VotePositionDO]] =
      voteId => positionRepo.findByVoteId(voteId).transact(resources.xa)

    val changeDetector = new VoteChangeDetector[F](findStoredVote, findStoredPositions, logger)

    new VoteProcessor[F](
      houseClient = houseApiClient,
      senateClient = senateXmlClient,
      lisResolver = lisResolver,
      houseConverter = houseConverter,
      senateConverter = senateConverter,
      changeDetector = changeDetector,
      persister = persister,
      billLookup = billLookup,
      memberLookup = memberLookup,
      eventEmitter = eventEmitter,
      findStoredVote = findStoredVote,
      houseConfig = config.pipeline.house,
      senateConfig = config.pipeline.senate,
      congress = config.pipeline.house.congress,
      session = config.pipeline.house.session,
      logger = logger,
    )
  }

  /**
   * Extract the step-level identifier from `args(2)` and parse it as a `Long`. The launcher is responsible for creating
   * the `workflow_run_steps` row and passing its BIGSERIAL PK before invoking the pipeline — a missing or non-numeric
   * value indicates a broken launcher contract and fails the run fast via [[StepRunIdInvalid]].
   */
  private[app] def extractStepRunId[F[_]: Sync](args: List[String]): F[Long] =
    args.lift(2) match {
      case Some(raw) if raw.trim.nonEmpty =>
        raw.trim.toLongOption match {
          case Some(id) => Sync[F].pure(id)
          case None     => Sync[F].raiseError[Long](StepRunIdInvalid(raw))
        }
      case Some(raw) => Sync[F].raiseError[Long](StepRunIdInvalid(raw))
      case None      => Sync[F].raiseError[Long](StepRunIdInvalid("<missing>"))
    }

  /**
   * Wraps an HTTP client with per-client rate limiting: a semaphore ensures only one request is in-flight at a time,
   * with `delay` inserted after each request completes. Canonical pattern across RepCheck pipelines — each HTTP client
   * gets its own wrapper with its own configured delay (`house.pageDelay` vs. `senate.requestDelay`).
   */
  private[app] def rateLimitedClient[F[_]: Async](
    underlying: Client[F],
    delay: FiniteDuration,
  ): Resource[F, Client[F]] =
    Resource.eval(Semaphore[F](1)).map { sem =>
      Client[F] { request =>
        Resource.make(sem.acquire)(_ => Temporal[F].sleep(delay) >> sem.release) >>
          underlying.run(request)
      }
    }

}
