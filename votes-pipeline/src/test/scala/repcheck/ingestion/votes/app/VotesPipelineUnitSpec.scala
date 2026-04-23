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
import repcheck.ingestion.common.events.{EventPublisherConfig, IngestionEventPublisher}
import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}
import repcheck.ingestion.votes.config.{HouseVotesConfig, SenateVoteXmlConfig, VotesPipelineConfig}
import repcheck.ingestion.votes.errors.StepRunIdInvalid
import repcheck.ingestion.votes.pipeline.VoteProcessor
import repcheck.pipeline.models.errors.RetryConfig
import repcheck.pipeline.models.metadata.ProcessingResult

/**
 * Unit spec for [[VotesPipeline]]'s testable runtime. Verifies the factory-function wiring contract:
 *   1. `runWithFactories` invokes each factory exactly once, in the documented order (config → logger → resources →
 *      processor → stream). 2. Config values are threaded end-to-end (the AppConfig supplied by `configLoader` is the
 *      one handed to `resourceBuilder` and `processorFactory`). 3. Arg parsing surfaces [[StepRunIdInvalid]] for
 *      missing / non-numeric `args(2)`. 4. Exit code reflects the collected [[ProcessingResult]] stream (all succeeded
 *      → `ExitCode.Success`, any failed → `ExitCode.Error`).
 *
 * No real transactor, HTTP client, or Pub/Sub publisher is constructed — factories return Mockito stubs wrapped in
 * `Resource.pure`, so the spec exercises only the orchestration logic in `runWithFactories`.
 */
class VotesPipelineUnitSpec extends AnyFlatSpec with Matchers with MockitoSugar {

  private val runIdArg     = "run-abc"
  private val stepRunIdArg = "42"
  private val validArgs    = List("ignored-config-arg", runIdArg, stepRunIdArg)

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
        house = HouseVotesConfig(congress = 118, session = 1, parallelism = 1, pageDelay = 1.millis, lookbackDays = 0),
        senate = SenateVoteXmlConfig(),
      ),
      eventPublisher =
        EventPublisherConfig(projectId = "repcheck-test", topicName = "vote-events", source = "votes-pipeline"),
    )

  private def makeResources(): VotesPipeline.Resources[IO] =
    VotesPipeline.Resources(
      xa = mock[Transactor[IO]],
      houseClient = mock[Client[IO]],
      senateClient = mock[Client[IO]],
      eventPublisher = mock[IngestionEventPublisher[IO]],
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

  "runWithFactories" should "invoke each factory exactly once in config → logger → resources → processor → stream order" in {
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
        streamFactory = (_, _) => { trace.record("streamFactory"); stream },
      )
      .unsafeRunSync()

    val _ = trace.events shouldBe List(
      "configLoader",
      "loggerFactory",
      "resourceBuilder",
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
        streamFactory = (_, _) => Stream.empty,
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
          (_: VotesPipeline.AppConfig) => Resource.pure[IO, VotesPipeline.Resources[IO]](makeResources()),
        processorFactory = (_, _, _) => mock[VoteProcessor[IO]],
        streamFactory = (_, rid) => {
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
    val resourcesSeen = new AtomicReference[Option[VotesPipeline.Resources[IO]]](None)
    val loggerSeen    = new AtomicReference[Option[PipelineLogger[IO]]](None)

    val _ = VotesPipeline
      .runWithFactories[IO](
        args = validArgs,
        configLoader = IO.pure(makeAppConfig()),
        loggerFactory = IO.pure(logger),
        resourceBuilder = (_: VotesPipeline.AppConfig) => Resource.pure[IO, VotesPipeline.Resources[IO]](resources),
        processorFactory = (_, r, l) => {
          val _ = resourcesSeen.set(Some(r))
          val _ = loggerSeen.set(Some(l))
          mock[VoteProcessor[IO]]
        },
        streamFactory = (_, _) => Stream.empty,
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
          (_: VotesPipeline.AppConfig) => Resource.pure[IO, VotesPipeline.Resources[IO]](makeResources()),
        processorFactory = (_, _, _) => mock[VoteProcessor[IO]],
        streamFactory = (_, _) => successStream,
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
          (_: VotesPipeline.AppConfig) => Resource.pure[IO, VotesPipeline.Resources[IO]](makeResources()),
        processorFactory = (_, _, _) => mock[VoteProcessor[IO]],
        streamFactory = (_, _) => mixedStream,
      )
      .unsafeRunSync()

    exitCode.code shouldBe 1
  }

  // =====================================================================================
  // Arg parsing — extractStepRunId
  // =====================================================================================

  "extractStepRunId" should "return the Long value when args(2) is numeric" in {
    VotesPipeline.extractStepRunId[IO](List("cfg", "run", "7")).unsafeRunSync() shouldBe 7L
  }

  it should "raise StepRunIdInvalid when args(2) is missing" in {
    val outcome = VotesPipeline.extractStepRunId[IO](List("cfg", "run")).attempt.unsafeRunSync()
    outcome match {
      case Left(e: StepRunIdInvalid) => e.getMessage should include("<missing>")
      case other                     => fail(s"expected Left(StepRunIdInvalid), got $other")
    }
  }

  it should "raise StepRunIdInvalid when args(2) is blank" in {
    val outcome = VotesPipeline.extractStepRunId[IO](List("cfg", "run", "   ")).attempt.unsafeRunSync()
    outcome match {
      case Left(e: StepRunIdInvalid) =>
        val _ = e.rawValue shouldBe "   "
        e.getMessage should include("invalid or missing")
      case other => fail(s"expected Left(StepRunIdInvalid), got $other")
    }
  }

  it should "raise StepRunIdInvalid when args(2) is not a number" in {
    val outcome = VotesPipeline.extractStepRunId[IO](List("cfg", "run", "not-a-long")).attempt.unsafeRunSync()
    outcome match {
      case Left(e: StepRunIdInvalid) => e.getMessage should include("not-a-long")
      case other                     => fail(s"expected Left(StepRunIdInvalid), got $other")
    }
  }

  it should "trim whitespace around a valid numeric args(2)" in {
    VotesPipeline.extractStepRunId[IO](List("cfg", "run", "  99  ")).unsafeRunSync() shouldBe 99L
  }

  // =====================================================================================
  // rateLimitedClient — mechanical behaviour
  // =====================================================================================

  "rateLimitedClient" should "produce a Client[F] backed by the supplied underlying and not run it eagerly" in {
    val underlying = mock[Client[IO]]
    // Just acquire the Resource and release it — we want to prove that constructing the wrapper has no immediate
    // side effects on the underlying client. If the wrapper were pre-running a request, Mockito would record it.
    val wrapperResource = VotesPipeline.rateLimitedClient[IO](underlying, 1.millis)
    val _               = wrapperResource.use(_ => IO.pure(true)).unsafeRunSync() shouldBe true
    // Mockito's default behavior on an unverified mock: no interactions — confirmed by the absence of any stubbing.
    org.mockito.Mockito.verifyNoInteractions(underlying)
  }

  // =====================================================================================
  // buildProcessor — wiring smoke test
  // =====================================================================================

  "buildProcessor" should "construct a fully-wired VoteProcessor from a Resources bundle without throwing" in {
    // Smoke test guards against latent wiring regressions (null-defaulted dep, wrong constructor arity, etc.) that the
    // compiler could miss. Invokes the real `buildProcessor` against mocked Resources — no DB, no HTTP, no Pub/Sub —
    // and asserts the returned VoteProcessor is a valid instance. Every downstream collaborator (clients, repos,
    // converters, persister, emitter, detector) is constructed inline and must resolve its constructor args correctly.
    val config    = makeAppConfig()
    val resources = makeResources()
    val logger    = makeLogger()
    val processor = VotesPipeline.buildProcessor[IO](config, resources, logger)
    processor shouldBe a[VoteProcessor[?]]
  }

}
