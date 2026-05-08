package repcheck.ingestion.amendments.textcheck.app

import java.util.UUID

import cats.effect.unsafe.implicits.global
import cats.effect.{IO, Resource}

import org.http4s.client.Client

import fs2.Stream

import doobie._

import pureconfig.ConfigSource

import org.mockito.ArgumentMatchers.anyLong
import org.mockito.Mockito.when
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar
import repcheck.ingestion.amendments.textcheck.app.AmendmentTextCheckerRun.AppConfig
import repcheck.ingestion.amendments.textcheck.config.AmendmentTextCheckerConfig
import repcheck.ingestion.amendments.textcheck.pipeline.AmendmentTextAvailabilityChecker
import repcheck.ingestion.common.api.CongressGovClientConfig
import repcheck.ingestion.common.db.DatabaseConfig
import repcheck.ingestion.common.events.{EventPublisherConfig, PubSubEventPublisher}
import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}
import repcheck.pipeline.models.errors.{ErrorClass, RetryConfig}
import repcheck.pipeline.models.metadata.ProcessingResult

class AmendmentTextCheckerRunSpec extends AnyFlatSpec with Matchers with MockitoSugar {

  private val testXa: Transactor[IO] = Transactor.fromDriverManager[IO](
    driver = "org.h2.Driver",
    url = "jdbc:h2:mem:runspec;DB_CLOSE_DELAY=-1",
    user = "",
    password = "",
    logHandler = None,
  )

  private val testConfig = AppConfig(
    database = DatabaseConfig(
      host = "localhost",
      port = 5432,
      database = "repcheck_test",
      username = "repcheck",
      password = "repcheck",
      maxConnections = 2,
    ),
    congressApi = CongressGovClientConfig(
      apiKey = "test-api-key",
      baseUrl = "http://localhost:8080",
      retry = RetryConfig(
        maxRetries = 1,
        initialBackoffMs = 1,
        maxBackoffMs = 10,
        backoffMultiplier = 1.0,
      ),
    ),
    pipeline = AmendmentTextCheckerConfig(),
    eventPublisher = EventPublisherConfig(
      projectId = "repcheck-test",
      topicName = "test-amendment-text",
      source = "checker-test",
    ),
  )

  private class StubPipelineLogger extends PipelineLogger[IO] {
    private val messagesRef = new java.util.concurrent.atomic.AtomicReference[List[String]](List.empty)

    override def info(context: LogContext, message: String): IO[Unit] = IO {
      val _ = messagesRef.updateAndGet(msgs => msgs :+ s"INFO: $message")
    }

    override def warn(context: LogContext, message: String): IO[Unit] = IO {
      val _ = messagesRef.updateAndGet(msgs => msgs :+ s"WARN: $message")
    }

    override def error(context: LogContext, message: String, cause: Option[Throwable]): IO[Unit] = IO {
      val _ = messagesRef.updateAndGet(msgs => msgs :+ s"ERROR: $message")
    }

    override def debug(context: LogContext, message: String): IO[Unit] = IO {
      val _ = messagesRef.updateAndGet(msgs => msgs :+ s"DEBUG: $message")
    }

    def messages: List[String] = messagesRef.get()
  }

  private val stubPubSub: PubSubEventPublisher[IO] = new PubSubEventPublisher[IO] {
    def publish(topic: String, data: String, attributes: Map[String, String]): IO[String] =
      IO.pure(s"stub-msg-${UUID.randomUUID().toString}")
  }

  private def stubResources(): AmendmentTextCheckerResources[IO] =
    AmendmentTextCheckerResources(
      xa = testXa,
      httpClient = Client.fromHttpApp(org.http4s.HttpApp.notFound[IO]),
      pubSubPublisher = stubPubSub,
    )

  "AppConfig" should "load from PureConfig reference + test override" in {
    val result = ConfigSource
      .resources("application-test.conf")
      .withFallback(ConfigSource.resources("application.conf"))
      .load[AppConfig]
    val _ = withClue(s"Config load failed: ${result.left.map(_.prettyPrint(0)).fold(identity, _ => "")}")(
      result.isRight shouldBe true
    )
  }

  "runWithFactories" should "complete successfully with empty stream" in {
    val logger = new StubPipelineLogger
    val exit = AmendmentTextCheckerRun
      .runWithFactories[IO](
        configLoader = IO.pure(testConfig),
        loggerFactory = (_: String) => IO.pure(logger),
        resourceBuilder = (_: AppConfig, _: PipelineLogger[IO]) =>
          Resource.pure[IO, AmendmentTextCheckerResources[IO]](stubResources()),
        checkerFactory = (_, _, _, _, _) => mock[AmendmentTextAvailabilityChecker[IO]],
        streamFactory = (_, _, _) => Stream.empty,
      )
      .unsafeRunSync()
    exit.code shouldBe 0
  }

  it should "propagate config-loader failures" in {
    val logger = new StubPipelineLogger
    val result = AmendmentTextCheckerRun
      .runWithFactories[IO](
        configLoader = IO.raiseError(new RuntimeException("Config load failed")),
        loggerFactory = (_: String) => IO.pure(logger),
        resourceBuilder = (_: AppConfig, _: PipelineLogger[IO]) =>
          Resource.pure[IO, AmendmentTextCheckerResources[IO]](stubResources()),
        checkerFactory = (_, _, _, _, _) => mock[AmendmentTextAvailabilityChecker[IO]],
        streamFactory = (_, _, _) => Stream.empty,
      )
      .attempt
      .unsafeRunSync()
    val _ = result.isLeft shouldBe true
    result.left.map(_.getMessage) shouldBe Left("Config load failed")
  }

  it should "log a pipeline-completed summary" in {
    val logger = new StubPipelineLogger
    val _ = AmendmentTextCheckerRun
      .runWithFactories[IO](
        configLoader = IO.pure(testConfig),
        loggerFactory = (_: String) => IO.pure(logger),
        resourceBuilder = (_: AppConfig, _: PipelineLogger[IO]) =>
          Resource.pure[IO, AmendmentTextCheckerResources[IO]](stubResources()),
        checkerFactory = (_, _, _, _, _) => mock[AmendmentTextAvailabilityChecker[IO]],
        streamFactory = (_, _, _) => Stream.empty,
      )
      .unsafeRunSync()
    logger.messages.exists(_.contains("Pipeline completed")) shouldBe true
  }

  "noOpRetryLogger" should "return F[Unit] regardless of arguments" in {
    val result = AmendmentTextCheckerRun
      .noOpRetryLogger[IO]
      .apply(1, 3, 100L, ErrorClass.Transient, "test", UUID.randomUUID())
      .unsafeRunSync()
    result shouldBe ((): Unit)
  }

  "buildChecker" should "construct an AmendmentTextAvailabilityChecker with all dependencies" in {
    val logger     = new StubPipelineLogger
    val httpClient = Client.fromHttpApp[IO](org.http4s.HttpApp.notFound[IO])
    val checker = AmendmentTextCheckerRun.buildChecker[IO](
      httpClient,
      testXa,
      stubPubSub,
      testConfig,
      logger,
    )
    checker.toString should not be empty
  }

  "buildResources" should "create a resource bundle from factory functions" in {
    val logger     = new StubPipelineLogger
    val httpClient = Client.fromHttpApp[IO](org.http4s.HttpApp.notFound[IO])
    val _ = AmendmentTextCheckerRun
      .buildResources[IO](
        config = testConfig,
        logger = logger,
        transactorFactory = (_: DatabaseConfig) => Resource.pure[IO, Transactor[IO]](testXa),
        httpClientFactory = Resource.pure[IO, Client[IO]](httpClient),
        pubSubPublisherFactory = (_: EventPublisherConfig) => Resource.pure[IO, PubSubEventPublisher[IO]](stubPubSub),
      )
      .use { res =>
        IO {
          val _ = res.xa.toString should not be empty
          val _ = res.httpClient.toString should not be empty
          res.pubSubPublisher.toString should not be empty
        }
      }
      .unsafeRunSync()
  }

  it should "pass each config section to the matching factory" in {
    val logger = new StubPipelineLogger
    val capturedDb =
      new java.util.concurrent.atomic.AtomicReference[DatabaseConfig](DatabaseConfig("", 0, "", "", "", 0))
    val capturedEv =
      new java.util.concurrent.atomic.AtomicReference[EventPublisherConfig](EventPublisherConfig("", "", ""))
    val httpClient = Client.fromHttpApp[IO](org.http4s.HttpApp.notFound[IO])

    val _ = AmendmentTextCheckerRun
      .buildResources[IO](
        config = testConfig,
        logger = logger,
        transactorFactory = (cfg: DatabaseConfig) => {
          capturedDb.set(cfg)
          Resource.pure[IO, Transactor[IO]](testXa)
        },
        httpClientFactory = Resource.pure[IO, Client[IO]](httpClient),
        pubSubPublisherFactory = (cfg: EventPublisherConfig) => {
          capturedEv.set(cfg)
          Resource.pure[IO, PubSubEventPublisher[IO]](stubPubSub)
        },
      )
      .use(_ => IO.unit)
      .unsafeRunSync()

    val _ = capturedDb.get.host shouldBe "localhost"
    val _ = capturedDb.get.database shouldBe "repcheck_test"
    val _ = capturedEv.get.topicName shouldBe "test-amendment-text"
    capturedEv.get.source shouldBe "checker-test"
  }

  "buildStream" should "delegate to checker.checkAll" in {
    val logger         = new StubPipelineLogger
    val checker        = mock[AmendmentTextAvailabilityChecker[IO]]
    val expectedResult = ProcessingResult.Succeeded(entityId = "117-SAMDT-1")
    when(checker.checkAll(anyLong())).thenReturn(Stream.emit(expectedResult))

    val results = AmendmentTextCheckerRun.buildStream[IO](checker, logger, 0L).compile.toList.unsafeRunSync()
    val _       = results.size shouldBe 1
    results.headOption.map(_.entityId) shouldBe Some("117-SAMDT-1")
  }

  it should "return empty stream when checker emits nothing" in {
    val logger  = new StubPipelineLogger
    val checker = mock[AmendmentTextAvailabilityChecker[IO]]
    when(checker.checkAll(anyLong())).thenReturn(Stream.empty)
    AmendmentTextCheckerRun.buildStream[IO](checker, logger, 0L).compile.toList.unsafeRunSync() shouldBe empty
  }

  it should "pass the runId through to checker.checkAll" in {
    val logger  = new StubPipelineLogger
    val checker = mock[AmendmentTextAvailabilityChecker[IO]]
    val captured =
      new java.util.concurrent.atomic.AtomicReference[Long](-1L)
    when(checker.checkAll(anyLong())).thenAnswer { invocation =>
      captured.set(invocation.getArgument[Long](0))
      Stream.empty
    }
    val _ = AmendmentTextCheckerRun.buildStream[IO](checker, logger, 999L).compile.drain.unsafeRunSync()
    captured.get shouldBe 999L
  }

  "loggingRetryLogger" should "emit a WARN with attempt/max/delay/correlationId context" in {
    val logger      = new StubPipelineLogger
    val correlation = UUID.fromString("11111111-2222-3333-4444-555555555555")
    val _ = AmendmentTextCheckerRun
      .loggingRetryLogger[IO](logger, "amendment-text-availability-checker")
      .apply(2, 5, 250L, ErrorClass.Transient, "boom", correlation)
      .unsafeRunSync()
    val warns = logger.messages.filter(_.startsWith("WARN:"))
    val _     = warns.size shouldBe 1
    val msg   = warns.headOption.getOrElse("")
    val _     = msg should include("Retry 2/5")
    val _     = msg should include("250ms")
    val _     = msg should include("Transient")
    msg should include("boom")
  }

}
