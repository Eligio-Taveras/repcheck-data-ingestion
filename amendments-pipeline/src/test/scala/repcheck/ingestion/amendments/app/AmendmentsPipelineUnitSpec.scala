package repcheck.ingestion.amendments.app

import java.util.concurrent.atomic.AtomicReference

import scala.concurrent.duration._

import cats.effect.unsafe.implicits.global
import cats.effect.{IO, Resource}

import org.http4s.client.Client

import fs2.Stream

import doobie.util.transactor.Transactor

import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.when
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar
import repcheck.ingestion.amendments.config.AmendmentsConfig
import repcheck.ingestion.amendments.errors.PoolSizingTooSmall
import repcheck.ingestion.amendments.pipeline.{AmendmentProcessor, PipelineRunSummary, VoteAmendmentLinker}
import repcheck.ingestion.common.api.CongressGovClientConfig
import repcheck.ingestion.common.db.DatabaseConfig
import repcheck.ingestion.common.errors.{RunIdMissing, StepRunIdInvalid}
import repcheck.ingestion.common.execution.WorkflowStateUpdater
import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}
import repcheck.pipeline.models.metadata.ProcessingResult

import com.repcheck.utils.errors.{RetryConfig, RetryWrapper}

class AmendmentsPipelineUnitSpec extends AnyFlatSpec with Matchers with MockitoSugar {

  // --------------------------------------------------------------------------------------------
  // Helpers
  // --------------------------------------------------------------------------------------------

  private val runId     = "123"
  private val validArgs = List("{}", runId, "0")

  private def appConfig(
    parallelism: Int = 4,
    maxRecursionDepth: Int = 10,
    maxConnections: Int = 45,
  ): AmendmentsPipeline.AppConfig =
    AmendmentsPipeline.AppConfig(
      database = DatabaseConfig(
        host = "localhost",
        port = 5432,
        database = "repcheck_test",
        username = "test",
        password = "test",
        maxConnections = maxConnections,
      ),
      congressApi = CongressGovClientConfig(
        baseUrl = "http://localhost:0",
        apiKey = "test-key",
        pageSize = 10,
        pageDelay = 1.millis,
        retry = RetryConfig(),
      ),
      pipeline = AmendmentsConfig(
        congressesMin = 117,
        congressesMax = 117,
        lookbackDays = 7,
        parallelism = parallelism,
        pageDelay = 0.millis,
        maxRecursionDepth = maxRecursionDepth,
        pageSize = 10,
      ),
    )

  private def silentLogger(): PipelineLogger[IO] = new PipelineLogger[IO] {
    override def info(context: LogContext, message: String): IO[Unit]                            = IO.unit
    override def warn(context: LogContext, message: String): IO[Unit]                            = IO.unit
    override def error(context: LogContext, message: String, cause: Option[Throwable]): IO[Unit] = IO.unit
    override def debug(context: LogContext, message: String): IO[Unit]                           = IO.unit
  }

  private def stubResources(): AmendmentsPipelineResources.Resources[IO] =
    AmendmentsPipelineResources.Resources(
      xa = Transactor.fromDriverManager[IO](
        driver = "org.h2.Driver",
        url = "jdbc:h2:mem:pipeline-spec;DB_CLOSE_DELAY=-1",
        user = "",
        password = "",
        logHandler = None,
      ),
      httpClient = mock[Client[IO]],
      retryWrapper = new RetryWrapper[IO]((_, _, _, _, _, _) => IO.unit),
    )

  private def stubProcessor(): AmendmentProcessor[IO] = {
    val p = mock[AmendmentProcessor[IO]]
    when(p.streamAll(any[String])).thenReturn(Stream.empty)
    when(p.summarize(any[List[ProcessingResult]])).thenAnswer { inv =>
      val results = inv.getArgument[List[ProcessingResult]](0)
      PipelineRunSummary(
        totalProcessed = results.size,
        succeeded = results.count(_.isSucceeded),
        skipped = results.count(_.isSkipped),
        failed = results.count(_.isFailed),
        eventsEmitted = 0,
        errors = results.collect { case f: ProcessingResult.Failed => f.reason },
      )
    }
    p
  }

  private class TraceWorkflowUpdater(events: AtomicReference[List[String]])
      extends WorkflowStateUpdater[IO](
        Transactor.fromDriverManager[IO](
          driver = "org.h2.Driver",
          url = "jdbc:h2:mem:trace;DB_CLOSE_DELAY=-1",
          user = "",
          password = "",
          logHandler = None,
        ),
        repcheck.ingestion.common.execution.PipelineFailureHandlerConfig(maxRetries = 3),
      ) {

    override def recordStepStarted(rid: Long, stepName: String): IO[Unit] = IO {
      val _ = events.updateAndGet(_ :+ s"started:${rid.toString}:$stepName")
    }

    override def recordStepCompleted(rid: Long, stepName: String): IO[Unit] = IO {
      val _ = events.updateAndGet(_ :+ s"completed:${rid.toString}:$stepName")
    }

    override def recordStepFailed(rid: Long, stepName: String, error: String): IO[Unit] = IO {
      val _ = events.updateAndGet(_ :+ s"failed:${rid.toString}:$stepName")
    }

  }

  /** Records into the shared trace when invoked; optional failure injection for the non-fatal path. */
  private class TraceLinker(events: AtomicReference[List[String]], failure: Option[Throwable] = None)
      extends VoteAmendmentLinker[IO] {

    override def linkAll(ctx: LogContext): IO[Int] = IO {
      val _ = events.updateAndGet(_ :+ "linker")
    } *> failure.fold(IO.pure(0))(IO.raiseError[Int])

  }

  // --------------------------------------------------------------------------------------------
  // Specs
  // --------------------------------------------------------------------------------------------

  "runWithFactories" should "invoke factories in config → poolValidate → logger → resources → processor → stream order" in {
    val trace = new AtomicReference[List[String]](List.empty)
    val cfg   = appConfig()

    val exit = AmendmentsPipeline
      .runWithFactories[IO](
        args = validArgs,
        configLoader = IO {
          val _ = trace.updateAndGet(_ :+ "configLoader"); cfg
        },
        loggerFactory = IO {
          val _ = trace.updateAndGet(_ :+ "loggerFactory"); silentLogger()
        },
        retryWrapperFactory = (_: PipelineLogger[IO]) => {
          val _ = trace.updateAndGet(_ :+ "retryWrapperFactory")
          new RetryWrapper[IO]((_, _, _, _, _, _) => IO.unit)
        },
        resourceBuilder = (_, _) =>
          Resource.eval(IO {
            val _ = trace.updateAndGet(_ :+ "resourceBuilder"); stubResources()
          }),
        processorFactory = (_, _, _, _) => {
          val _ = trace.updateAndGet(_ :+ "processorFactory")
          stubProcessor()
        },
        streamFactory = (_, _) => {
          val _ = trace.updateAndGet(_ :+ "streamFactory")
          Stream.empty
        },
        stepRecorderFactory = _ => new TraceWorkflowUpdater(trace),
        linkerFactory = (_, _) => new TraceLinker(trace),
      )
      .unsafeRunSync()

    val events = trace.get()
    val _      = events.indexOf("configLoader") should be < events.indexOf("loggerFactory")
    val _      = events.indexOf("loggerFactory") should be < events.indexOf("resourceBuilder")
    val _      = events.indexOf("resourceBuilder") should be < events.indexOf("processorFactory")
    val _      = events.indexOf("processorFactory") should be < events.indexOf("streamFactory")
    exit.code shouldBe 0
  }

  it should "record step completion when no failures occurred" in {
    val trace = new AtomicReference[List[String]](List.empty)

    val _ = AmendmentsPipeline
      .runWithFactories[IO](
        args = validArgs,
        configLoader = IO.pure(appConfig()),
        loggerFactory = IO.pure(silentLogger()),
        retryWrapperFactory = (_: PipelineLogger[IO]) => new RetryWrapper[IO]((_, _, _, _, _, _) => IO.unit),
        resourceBuilder = (_, _) => Resource.pure[IO, AmendmentsPipelineResources.Resources[IO]](stubResources()),
        processorFactory = (_, _, _, _) => stubProcessor(),
        streamFactory = (_, _) => Stream.emit(ProcessingResult.Succeeded("a-1")),
        stepRecorderFactory = _ => new TraceWorkflowUpdater(trace),
        linkerFactory = (_, _) => new TraceLinker(trace),
      )
      .unsafeRunSync()

    val _      = trace.get() should contain(s"started:$runId:amendments-pipeline")
    val _      = trace.get() should contain(s"completed:$runId:amendments-pipeline")
    val events = trace.get()
    // The vote↔amendment linker runs on the success path, before completion is recorded.
    val _ = events should contain("linker")
    events.indexOf("linker") should be < events.indexOf(s"completed:$runId:amendments-pipeline")
  }

  it should "skip the vote↔amendment linker when the run failed" in {
    val trace = new AtomicReference[List[String]](List.empty)

    val _ = AmendmentsPipeline
      .runWithFactories[IO](
        args = validArgs,
        configLoader = IO.pure(appConfig()),
        loggerFactory = IO.pure(silentLogger()),
        retryWrapperFactory = (_: PipelineLogger[IO]) => new RetryWrapper[IO]((_, _, _, _, _, _) => IO.unit),
        resourceBuilder = (_, _) => Resource.pure[IO, AmendmentsPipelineResources.Resources[IO]](stubResources()),
        processorFactory = (_, _, _, _) => stubProcessor(),
        streamFactory = (_, _) => Stream.emit(ProcessingResult.Failed("a-1", "boom")),
        stepRecorderFactory = _ => new TraceWorkflowUpdater(trace),
        linkerFactory = (_, _) => new TraceLinker(trace),
      )
      .unsafeRunSync()

    trace.get() should not contain "linker"
  }

  it should "treat a linker failure as non-fatal (run still completes successfully)" in {
    val trace = new AtomicReference[List[String]](List.empty)

    val exit = AmendmentsPipeline
      .runWithFactories[IO](
        args = validArgs,
        configLoader = IO.pure(appConfig()),
        loggerFactory = IO.pure(silentLogger()),
        retryWrapperFactory = (_: PipelineLogger[IO]) => new RetryWrapper[IO]((_, _, _, _, _, _) => IO.unit),
        resourceBuilder = (_, _) => Resource.pure[IO, AmendmentsPipelineResources.Resources[IO]](stubResources()),
        processorFactory = (_, _, _, _) => stubProcessor(),
        streamFactory = (_, _) => Stream.emit(ProcessingResult.Succeeded("a-1")),
        stepRecorderFactory = _ => new TraceWorkflowUpdater(trace),
        linkerFactory = (_, _) => new TraceLinker(trace, failure = Some(new RuntimeException("link boom"))),
      )
      .unsafeRunSync()

    // Linker raised, but the run is still a success and completion is recorded.
    val _ = exit.code shouldBe 0
    val _ = trace.get() should contain("linker")
    trace.get() should contain(s"completed:$runId:amendments-pipeline")
  }

  it should "record step failure when a result was Failed" in {
    val trace = new AtomicReference[List[String]](List.empty)

    val exit = AmendmentsPipeline
      .runWithFactories[IO](
        args = validArgs,
        configLoader = IO.pure(appConfig()),
        loggerFactory = IO.pure(silentLogger()),
        retryWrapperFactory = (_: PipelineLogger[IO]) => new RetryWrapper[IO]((_, _, _, _, _, _) => IO.unit),
        resourceBuilder = (_, _) => Resource.pure[IO, AmendmentsPipelineResources.Resources[IO]](stubResources()),
        processorFactory = (_, _, _, _) => stubProcessor(),
        streamFactory = (_, _) => Stream.emit(ProcessingResult.Failed("a-1", "boom")),
        stepRecorderFactory = _ => new TraceWorkflowUpdater(trace),
        linkerFactory = (_, _) => new TraceLinker(trace),
      )
      .unsafeRunSync()

    val _ = exit.code shouldBe 1
    trace.get() should contain(s"failed:$runId:amendments-pipeline")
  }

  it should "raise PoolSizingTooSmall when max-connections is below parallelism × maxRecursionDepth + 5" in {
    val undersized = appConfig(parallelism = 4, maxRecursionDepth = 10, maxConnections = 5)

    val outcome = AmendmentsPipeline
      .runWithFactories[IO](
        args = validArgs,
        configLoader = IO.pure(undersized),
        loggerFactory = IO.pure(silentLogger()),
        retryWrapperFactory = (_: PipelineLogger[IO]) => new RetryWrapper[IO]((_, _, _, _, _, _) => IO.unit),
        resourceBuilder = (_, _) => Resource.pure[IO, AmendmentsPipelineResources.Resources[IO]](stubResources()),
        processorFactory = (_, _, _, _) => stubProcessor(),
        streamFactory = (_, _) => Stream.empty,
        stepRecorderFactory = _ => new TraceWorkflowUpdater(new AtomicReference(List.empty)),
        linkerFactory = (_, _) => new TraceLinker(new AtomicReference(List.empty)),
      )
      .attempt
      .unsafeRunSync()

    outcome match {
      case Left(_: PoolSizingTooSmall) => succeed
      case other                       => fail(s"expected PoolSizingTooSmall, got $other")
    }
  }

  it should "raise RunIdMissing when args(1) is blank" in {
    val outcome = AmendmentsPipeline
      .runWithFactories[IO](
        args = List("cfg", ""),
        configLoader = IO.pure(appConfig()),
        loggerFactory = IO.pure(silentLogger()),
        retryWrapperFactory = (_: PipelineLogger[IO]) => new RetryWrapper[IO]((_, _, _, _, _, _) => IO.unit),
        resourceBuilder = (_, _) => Resource.pure[IO, AmendmentsPipelineResources.Resources[IO]](stubResources()),
        processorFactory = (_, _, _, _) => stubProcessor(),
        streamFactory = (_, _) => Stream.empty,
        stepRecorderFactory = _ => new TraceWorkflowUpdater(new AtomicReference(List.empty)),
        linkerFactory = (_, _) => new TraceLinker(new AtomicReference(List.empty)),
      )
      .attempt
      .unsafeRunSync()

    outcome match {
      case Left(_: RunIdMissing) => succeed
      case other                 => fail(s"expected RunIdMissing, got $other")
    }
  }

  it should "raise StepRunIdInvalid when args(2) is missing" in {
    val outcome = AmendmentsPipeline
      .runWithFactories[IO](
        args = List("{}", "123"),
        configLoader = IO.pure(appConfig()),
        loggerFactory = IO.pure(silentLogger()),
        retryWrapperFactory = (_: PipelineLogger[IO]) => new RetryWrapper[IO]((_, _, _, _, _, _) => IO.unit),
        resourceBuilder = (_, _) => Resource.pure[IO, AmendmentsPipelineResources.Resources[IO]](stubResources()),
        processorFactory = (_, _, _, _) => stubProcessor(),
        streamFactory = (_, _) => Stream.empty,
        stepRecorderFactory = _ => new TraceWorkflowUpdater(new AtomicReference(List.empty)),
        linkerFactory = (_, _) => new TraceLinker(new AtomicReference(List.empty)),
      )
      .attempt
      .unsafeRunSync()

    outcome match {
      case Left(_: StepRunIdInvalid) => succeed
      case other                     => fail(s"expected StepRunIdInvalid, got $other")
    }
  }

  // --------------------------------------------------------------------------------------------
  // Resources / pool-sizing helpers
  // --------------------------------------------------------------------------------------------

  "validatePoolSizing" should "return None when the pool is sized at exactly the required minimum" in {
    val cfg = AmendmentsConfig(parallelism = 4, maxRecursionDepth = 10)
    val db  = DatabaseConfig("h", 0, "db", "u", "p", maxConnections = 45)
    AmendmentsPipelineResources.validatePoolSizing(db, cfg) shouldBe None
  }

  it should "return Some(message) when the pool is undersized" in {
    val cfg = AmendmentsConfig(parallelism = 4, maxRecursionDepth = 10)
    val db  = DatabaseConfig("h", 0, "db", "u", "p", maxConnections = 10)
    val res = AmendmentsPipelineResources.validatePoolSizing(db, cfg)
    val _   = res.isDefined shouldBe true
    res.foreach(_ should include("45"))
  }

}
