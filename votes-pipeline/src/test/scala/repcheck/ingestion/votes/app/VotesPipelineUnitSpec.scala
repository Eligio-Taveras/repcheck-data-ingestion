package repcheck.ingestion.votes.app

import java.util.concurrent.atomic.AtomicReference

import scala.concurrent.duration._

import cats.effect.unsafe.implicits.global
import cats.effect.{IO, Resource}

import org.http4s.client.Client

import fs2.Stream

import doobie.util.transactor.Transactor

import org.mockito.ArgumentMatchers.{any, anyString}
import org.mockito.Mockito.when
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar
import repcheck.ingestion.common.api.CongressGovClientConfig
import repcheck.ingestion.common.db.DatabaseConfig
import repcheck.ingestion.common.errors.{RunIdMissing, StepRunIdInvalid}
import repcheck.ingestion.common.events.{EventPublisherConfig, IngestionEventPublisher}
import repcheck.ingestion.common.execution.PipelineBootstrap
import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}
import repcheck.ingestion.votes.config.{HouseVotesConfig, SenateVoteXmlConfig, VotesPipelineConfig}
import repcheck.ingestion.votes.pipeline.VoteProcessor
import repcheck.pipeline.models.metadata.ProcessingResult

import com.repcheck.utils.errors.RetryConfig

/**
 * Unit spec for [[VotesPipeline]]'s testable runtime + the companion factories
 * ([[VotesPipelineResources.rateLimitedClient]] + [[VotesProcessorFactory.build]]). Verifies the factory-function
 * wiring contract:
 *   1. `runWithFactories` invokes each factory exactly once, in the documented order (config → logger → resources →
 *      processor → stream). 2. Config values are threaded end-to-end. 3. Arg parsing surfaces `RunIdMissing` /
 *      `StepRunIdInvalid` for missing / non-numeric `args(1)` / `args(2)`. 4. Exit code reflects the collected
 *      [[ProcessingResult]] stream.
 *
 * No real transactor, HTTP client, or Pub/Sub publisher is constructed — factories return Mockito stubs wrapped in
 * `Resource.pure`, so the spec exercises only orchestration.
 */
class VotesPipelineUnitSpec extends AnyFlatSpec with Matchers with MockitoSugar {

  private val runIdArg     = "123"
  private val stepRunIdArg = "42"
  private val validArgs    = List("{}", runIdArg, stepRunIdArg)

  private def makeAppConfig(): VotesPipeline.AppConfig =
    VotesPipeline.AppConfig(
      database = DatabaseConfig(
        host = "localhost",
        port = 5432,
        database = "repcheck_test",
        username = "test",
        password = "test",
        maxConnections = 3,
      ),
      congressApi = CongressGovClientConfig(
        baseUrl = "http://localhost:0",
        apiKey = "test-key",
        pageSize = 10,
        pageDelay = 1.millis,
        retry = RetryConfig(),
      ),
      pipeline = VotesPipelineConfig(
        house = HouseVotesConfig(parallelism = 1, pageDelay = 1.millis, lookbackDays = 0),
        senate = SenateVoteXmlConfig(),
        congresses = List(118),
      ),
      eventPublisher =
        EventPublisherConfig(projectId = "repcheck-test", topicName = "vote-events", source = "votes-pipeline"),
    )

  private def makeResources(): VotesPipelineResources.Resources[IO] =
    VotesPipelineResources.Resources(
      xa = mock[Transactor[IO]],
      houseClient = mock[Client[IO]],
      senateClient = mock[Client[IO]],
      eventPublisher = mock[IngestionEventPublisher[IO]],
      retryWrapper = VotesPipelineResources.noOpRetryWrapper[IO],
    )

  private def makeLogger(): PipelineLogger[IO] = {
    val m = mock[PipelineLogger[IO]]
    when(m.info(any[LogContext], anyString())).thenReturn(IO.unit)
    when(m.warn(any[LogContext], anyString())).thenReturn(IO.unit)
    when(m.error(any[LogContext], anyString(), any[Option[Throwable]])).thenReturn(IO.unit)
    when(m.debug(any[LogContext], anyString())).thenReturn(IO.unit)
    m
  }

  // Small helper: record an event name onto an AtomicReference-backed trace. Uses AtomicReference rather than a
  // mutable.Buffer to satisfy WartRemover's mutable-collection restriction in test code.
  private class Trace {
    private val ref = new AtomicReference[List[String]](List.empty)

    def record(event: String): Unit = {
      val _ = ref.updateAndGet(events => events :+ event)
    }

    def events: List[String] = ref.get()
  }

  // =====================================================================================
  // Ordering + wiring
  // =====================================================================================

  "runWithFactories" should "invoke each factory exactly once in config → logger → resources → congressesResolver → processor → stream order" in {
    val trace     = new Trace
    val config    = makeAppConfig()
    val resources = makeResources()
    val processor = mock[VoteProcessor[IO]]
    val logger    = makeLogger()
    val stream    = Stream.emit[IO, ProcessingResult](ProcessingResult.Succeeded("v-1"))

    val exitCode = VotesPipeline
      .runWithFactories[IO](
        args = validArgs,
        configLoader = IO { trace.record("configLoader"); config },
        loggerFactory = IO { trace.record("loggerFactory"); logger },
        resourceBuilder =
          (_: VotesPipeline.AppConfig) => Resource.eval(IO { trace.record("resourceBuilder"); resources }),
        processorFactory = (_, _, _) => { trace.record("processorFactory"); processor },
        congressesResolver = (_, _, _) => IO { trace.record("congressesResolver"); List(118) },
        streamFactory = (_, _, _) => { trace.record("streamFactory"); stream },
      )
      .unsafeRunSync()

    val _ = trace.events shouldBe List(
      "configLoader",
      "loggerFactory",
      "resourceBuilder",
      "congressesResolver",
      "processorFactory",
      "streamFactory",
    )
    exitCode.code shouldBe 0
  }

  it should "thread the loaded AppConfig into both resourceBuilder and processorFactory" in {
    val config           = makeAppConfig()
    val resources        = makeResources()
    val configSeenByRB   = new AtomicReference[Option[VotesPipeline.AppConfig]](None)
    val configSeenByProc = new AtomicReference[Option[VotesPipeline.AppConfig]](None)

    val _ = VotesPipeline
      .runWithFactories[IO](
        args = validArgs,
        configLoader = IO.pure(config),
        loggerFactory = IO.pure(makeLogger()),
        resourceBuilder =
          (c: VotesPipeline.AppConfig) => Resource.eval(IO { val _ = configSeenByRB.set(Some(c)); resources }),
        processorFactory = (c: VotesPipeline.AppConfig, _, _) => {
          val _ = configSeenByProc.set(Some(c))
          mock[VoteProcessor[IO]]
        },
        congressesResolver = (_, _, _) => IO.pure(List(118)),
        streamFactory = (_, _, _) => Stream.empty,
      )
      .unsafeRunSync()

    val _ = configSeenByRB.get() shouldBe Some(config)
    configSeenByProc.get() shouldBe Some(config)
  }

  it should "thread the parsed runId into streamFactory" in {
    val runIdSeen = new AtomicReference[Option[String]](None)

    val _ = VotesPipeline
      .runWithFactories[IO](
        args = validArgs,
        configLoader = IO.pure(makeAppConfig()),
        loggerFactory = IO.pure(makeLogger()),
        resourceBuilder =
          (_: VotesPipeline.AppConfig) => Resource.pure[IO, VotesPipelineResources.Resources[IO]](makeResources()),
        processorFactory = (_, _, _) => mock[VoteProcessor[IO]],
        congressesResolver = (_, _, _) => IO.pure(List(118)),
        streamFactory = (_, rid, _) => {
          val _ = runIdSeen.set(Some(rid))
          Stream.empty
        },
      )
      .unsafeRunSync()

    runIdSeen.get() shouldBe Some(runIdArg)
  }

  it should "hand the Resources bundle and the logger produced by the factories into processorFactory" in {
    val resources     = makeResources()
    val logger        = makeLogger()
    val resourcesSeen = new AtomicReference[Option[VotesPipelineResources.Resources[IO]]](None)
    val loggerSeen    = new AtomicReference[Option[PipelineLogger[IO]]](None)

    val _ = VotesPipeline
      .runWithFactories[IO](
        args = validArgs,
        configLoader = IO.pure(makeAppConfig()),
        loggerFactory = IO.pure(logger),
        resourceBuilder =
          (_: VotesPipeline.AppConfig) => Resource.pure[IO, VotesPipelineResources.Resources[IO]](resources),
        processorFactory = (_, r, l) => {
          val _ = resourcesSeen.set(Some(r))
          val _ = loggerSeen.set(Some(l))
          mock[VoteProcessor[IO]]
        },
        congressesResolver = (_, _, _) => IO.pure(List(118)),
        streamFactory = (_, _, _) => Stream.empty,
      )
      .unsafeRunSync()

    val _ = resourcesSeen.get() shouldBe Some(resources)
    loggerSeen.get() shouldBe Some(logger)
  }

  // =====================================================================================
  // Exit-code mapping
  // =====================================================================================

  it should "return ExitCode.Success when every ProcessingResult succeeded" in {
    val successStream = Stream.emits[IO, ProcessingResult](
      List(ProcessingResult.Succeeded("v-1"), ProcessingResult.Succeeded("v-2"))
    )

    val exitCode = VotesPipeline
      .runWithFactories[IO](
        args = validArgs,
        configLoader = IO.pure(makeAppConfig()),
        loggerFactory = IO.pure(makeLogger()),
        resourceBuilder =
          (_: VotesPipeline.AppConfig) => Resource.pure[IO, VotesPipelineResources.Resources[IO]](makeResources()),
        processorFactory = (_, _, _) => mock[VoteProcessor[IO]],
        congressesResolver = (_, _, _) => IO.pure(List(118)),
        streamFactory = (_, _, _) => successStream,
      )
      .unsafeRunSync()

    exitCode.code shouldBe 0
  }

  it should "return ExitCode.Error when any ProcessingResult failed" in {
    val mixedStream = Stream.emits[IO, ProcessingResult](
      List(ProcessingResult.Succeeded("v-1"), ProcessingResult.Failed("v-2", "boom"))
    )

    val exitCode = VotesPipeline
      .runWithFactories[IO](
        args = validArgs,
        configLoader = IO.pure(makeAppConfig()),
        loggerFactory = IO.pure(makeLogger()),
        resourceBuilder =
          (_: VotesPipeline.AppConfig) => Resource.pure[IO, VotesPipelineResources.Resources[IO]](makeResources()),
        processorFactory = (_, _, _) => mock[VoteProcessor[IO]],
        congressesResolver = (_, _, _) => IO.pure(List(118)),
        streamFactory = (_, _, _) => mixedStream,
      )
      .unsafeRunSync()

    exitCode.code shouldBe 1
  }

  // =====================================================================================
  // Arg parsing — runWithFactories surfaces PipelineBootstrap.extractRunId / extractStepRunId
  // =====================================================================================

  private def runWithArgs(args: List[String]): Either[Throwable, Int] =
    VotesPipeline
      .runWithFactories[IO](
        args = args,
        configLoader = IO.pure(makeAppConfig()),
        loggerFactory = IO.pure(makeLogger()),
        resourceBuilder =
          (_: VotesPipeline.AppConfig) => Resource.pure[IO, VotesPipelineResources.Resources[IO]](makeResources()),
        processorFactory = (_, _, _) => mock[VoteProcessor[IO]],
        congressesResolver = (_, _, _) => IO.pure(List(118)),
        streamFactory = (_, _, _) => Stream.empty,
      )
      .attempt
      .map(_.map(_.code))
      .unsafeRunSync()

  "runWithFactories arg parsing" should "raise RunIdMissing when args(1) is non-numeric" in {
    runWithArgs(List("{}", "run-abc", "42")) match {
      case Left(e: RunIdMissing) => e.getMessage should include("run-abc")
      case other                 => fail(s"expected Left(RunIdMissing), got $other")
    }
  }

  it should "raise StepRunIdInvalid when args(2) is missing" in {
    runWithArgs(List("{}", "123")) match {
      case Left(e: StepRunIdInvalid) => e.getMessage should include("missing or invalid")
      case other                     => fail(s"expected Left(StepRunIdInvalid), got $other")
    }
  }

  it should "raise StepRunIdInvalid when args(2) is not a number" in {
    runWithArgs(List("{}", "123", "not-a-long")) match {
      case Left(e: StepRunIdInvalid) => e.getMessage should include("not-a-long")
      case other                     => fail(s"expected Left(StepRunIdInvalid), got $other")
    }
  }

  "PipelineBootstrap.extractStepRunId" should "return the Long value when args(2) is numeric" in {
    PipelineBootstrap.extractStepRunId[IO](List("{}", "123", "7")).unsafeRunSync() shouldBe 7L
  }

  it should "trim whitespace around a valid numeric args(2)" in {
    PipelineBootstrap.extractStepRunId[IO](List("{}", "123", "  99  ")).unsafeRunSync() shouldBe 99L
  }

  // =====================================================================================
  // (rateLimitedClient mechanics are now covered by ingestion-common's RateLimitedHttpClientSpec.)
  // =====================================================================================

  // =====================================================================================
  // VotesProcessorFactory.build — wiring smoke test
  // =====================================================================================

  "VotesProcessorFactory.build" should "construct a fully-wired VoteProcessor from a Resources bundle without throwing" in {
    // Smoke test guards against latent wiring regressions (null-defaulted dep, wrong constructor arity, etc.) that the
    // compiler could miss. Invokes the real `build` against mocked Resources — no DB, no HTTP, no Pub/Sub — and
    // asserts the returned VoteProcessor is a valid instance. Every downstream collaborator (clients, repos,
    // converters, persister, emitter, detector, lisResolver) is constructed inline and must resolve correctly.
    val config    = makeAppConfig()
    val resources = makeResources()
    val logger    = makeLogger()
    val processor = VotesProcessorFactory.build[IO](config, resources, logger)
    processor shouldBe a[VoteProcessor[?]]
  }

  // =====================================================================================
  // VotesProcessorFactory.buildFindStoredVote / buildFindStoredPositions — the extracted
  // callbacks that feed VoteChangeDetector and VoteProcessor. Covered directly so their
  // `repo → transact(xa)` wrapping runs under unit tests.
  // =====================================================================================

  "buildFindStoredVote" should "delegate to VoteRepository.findByNaturalKey and transact against the supplied xa" in {
    import doobie.free.connection
    import repcheck.ingestion.votes.repo.VoteRepository
    import repcheck.shared.models.congress.dos.vote.VoteDO

    val voteRepo = mock[VoteRepository]
    val voteDo   = mock[VoteDO]
    when(voteRepo.findByNaturalKey(org.mockito.ArgumentMatchers.eq("house:119:1:42")))
      .thenReturn(connection.pure(Some(voteDo)))

    val xa = Transactor.fromDriverManager[IO](
      driver = "org.h2.Driver",
      url = "jdbc:h2:mem:buildfindstoredvote;DB_CLOSE_DELAY=-1",
      user = "",
      password = "",
      logHandler = None,
    )

    val callback = VotesProcessorFactory.buildFindStoredVote[IO](voteRepo, xa)
    val result   = callback("house:119:1:42").unsafeRunSync()
    result shouldBe Some(voteDo)
  }

  it should "return None when the repository returns None" in {
    import doobie.free.connection
    import repcheck.ingestion.votes.repo.VoteRepository
    import repcheck.shared.models.congress.dos.vote.VoteDO

    val voteRepo = mock[VoteRepository]
    when(voteRepo.findByNaturalKey(org.mockito.ArgumentMatchers.eq("senate:119:1:7")))
      .thenReturn(connection.pure(Option.empty[VoteDO]))

    val xa = Transactor.fromDriverManager[IO](
      driver = "org.h2.Driver",
      url = "jdbc:h2:mem:buildfindstoredvote-none;DB_CLOSE_DELAY=-1",
      user = "",
      password = "",
      logHandler = None,
    )

    VotesProcessorFactory.buildFindStoredVote[IO](voteRepo, xa).apply("senate:119:1:7").unsafeRunSync() shouldBe None
  }

  "buildFindStoredPositions" should "delegate to VotePositionRepository.findByVoteId and transact against the supplied xa" in {
    import doobie.free.connection
    import repcheck.ingestion.votes.repo.VotePositionRepository
    import repcheck.shared.models.congress.dos.vote.VotePositionDO

    val positionRepo = mock[VotePositionRepository]
    val pos          = mock[VotePositionDO]
    when(positionRepo.findByVoteId(org.mockito.ArgumentMatchers.eq(42L)))
      .thenReturn(connection.pure(List(pos)))

    val xa = Transactor.fromDriverManager[IO](
      driver = "org.h2.Driver",
      url = "jdbc:h2:mem:buildfindstoredpositions;DB_CLOSE_DELAY=-1",
      user = "",
      password = "",
      logHandler = None,
    )

    val callback = VotesProcessorFactory.buildFindStoredPositions[IO](positionRepo, xa)
    callback(42L).unsafeRunSync() shouldBe List(pos)
  }

  it should "return an empty list when the repository returns none" in {
    import doobie.free.connection
    import repcheck.ingestion.votes.repo.VotePositionRepository
    import repcheck.shared.models.congress.dos.vote.VotePositionDO

    val positionRepo = mock[VotePositionRepository]
    when(positionRepo.findByVoteId(org.mockito.ArgumentMatchers.eq(0L)))
      .thenReturn(connection.pure(List.empty[VotePositionDO]))

    val xa = Transactor.fromDriverManager[IO](
      driver = "org.h2.Driver",
      url = "jdbc:h2:mem:buildfindstoredpositions-empty;DB_CLOSE_DELAY=-1",
      user = "",
      password = "",
      logHandler = None,
    )

    VotesProcessorFactory.buildFindStoredPositions[IO](positionRepo, xa).apply(0L).unsafeRunSync() shouldBe List.empty
  }

}
