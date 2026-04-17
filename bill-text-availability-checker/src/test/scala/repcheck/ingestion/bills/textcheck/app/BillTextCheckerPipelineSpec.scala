package repcheck.ingestion.bills.textcheck.app

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
import repcheck.ingestion.bills.textcheck.app.BillTextCheckerPipeline.{AppConfig, CheckerResources}
import repcheck.ingestion.bills.textcheck.config.BillTextCheckerConfig
import repcheck.ingestion.bills.textcheck.pipeline.BillTextAvailabilityChecker
import repcheck.ingestion.common.api.CongressGovClientConfig
import repcheck.ingestion.common.db.DatabaseConfig
import repcheck.ingestion.common.events.{EventPublisherConfig, PubSubEventPublisher}
import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}
import repcheck.pipeline.models.errors.{ErrorClass, RetryConfig}
import repcheck.pipeline.models.metadata.ProcessingResult

class BillTextCheckerPipelineSpec extends AnyFlatSpec with Matchers with MockitoSugar {

  private val testXa: Transactor[IO] = Transactor.fromDriverManager[IO](
    driver = "org.h2.Driver",
    url = "jdbc:h2:mem:checkerspec;DB_CLOSE_DELAY=-1",
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
    pipeline = BillTextCheckerConfig(
      parallelism = 1,
      eventPublishRetry = RetryConfig(
        maxRetries = 1,
        initialBackoffMs = 1,
        maxBackoffMs = 10,
        backoffMultiplier = 1.0,
      ),
    ),
    eventPublisher = EventPublisherConfig(
      projectId = "repcheck-test",
      topicName = "test-bill-events",
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
      IO.pure(s"stub-msg-${UUID.randomUUID()}")
  }

  private def stubResources(): CheckerResources[IO] =
    CheckerResources(
      xa = testXa,
      httpClient = Client.fromHttpApp(org.http4s.HttpApp.notFound[IO]),
      pubSubPublisher = stubPubSub,
    )

  "AppConfig" should "load from PureConfig reference configuration" in {
    val result = ConfigSource
      .resources("application-test.conf")
      .withFallback(ConfigSource.resources("application.conf"))
      .load[AppConfig]
    val _ = withClue(s"Config load failed: ${result.left.map(_.prettyPrint(0))}")(
      result.isRight shouldBe true
    )
  }

  "runWithFactories" should "complete successfully with empty stream" in {
    val logger = new StubPipelineLogger

    val exitCode = BillTextCheckerPipeline
      .runWithFactories[IO](
        configLoader = IO.pure(testConfig),
        loggerFactory = (_: String) => IO.pure(logger),
        resourceBuilder =
          (_: AppConfig, _: PipelineLogger[IO]) => Resource.pure[IO, CheckerResources[IO]](stubResources()),
        checkerFactory = (_, _, _, _, _) => mock[BillTextAvailabilityChecker[IO]],
        streamFactory = (_, _) => Stream.empty,
      )
      .unsafeRunSync()

    exitCode.code shouldBe 0
  }

  it should "propagate config loading failures" in {
    val logger = new StubPipelineLogger

    val result = BillTextCheckerPipeline
      .runWithFactories[IO](
        configLoader = IO.raiseError(new RuntimeException("Config load failed")),
        loggerFactory = (_: String) => IO.pure(logger),
        resourceBuilder =
          (_: AppConfig, _: PipelineLogger[IO]) => Resource.pure[IO, CheckerResources[IO]](stubResources()),
        checkerFactory = (_, _, _, _, _) => mock[BillTextAvailabilityChecker[IO]],
        streamFactory = (_, _) => Stream.empty,
      )
      .attempt
      .unsafeRunSync()

    val _ = result.isLeft shouldBe true
    result.left.map(_.getMessage) shouldBe Left("Config load failed")
  }

  it should "log pipeline summary after execution" in {
    val logger = new StubPipelineLogger

    val _ = BillTextCheckerPipeline
      .runWithFactories[IO](
        configLoader = IO.pure(testConfig),
        loggerFactory = (_: String) => IO.pure(logger),
        resourceBuilder =
          (_: AppConfig, _: PipelineLogger[IO]) => Resource.pure[IO, CheckerResources[IO]](stubResources()),
        checkerFactory = (_, _, _, _, _) => mock[BillTextAvailabilityChecker[IO]],
        streamFactory = (_, _) => Stream.empty,
      )
      .unsafeRunSync()

    val summaryLogs = logger.messages.filter(_.contains("Pipeline completed"))
    summaryLogs should not be empty
  }

  "noOpRetryLogger" should "return F[Unit] regardless of arguments" in {
    // Directly invoke the extracted retry logger so its body is exercised — the production
    // retry path wires this through RetryWrapper, but no unit test triggers an actual retry,
    // leaving the lambda uncovered when inlined. Extracting to a named method lets tests
    // cover the body without having to simulate transient failures.
    val result = BillTextCheckerPipeline
      .noOpRetryLogger[IO]
      .apply(1, 3, 100L, ErrorClass.Transient, "test error", UUID.randomUUID())
      .unsafeRunSync()

    result shouldBe ((): Unit)
  }

  "buildChecker" should "construct a BillTextAvailabilityChecker with all dependencies" in {
    val logger     = new StubPipelineLogger
    val httpClient = Client.fromHttpApp[IO](org.http4s.HttpApp.notFound[IO])

    val checker = BillTextCheckerPipeline.buildChecker[IO](
      httpClient,
      testXa,
      stubPubSub,
      testConfig,
      logger,
    )

    checker.toString should not be empty
  }

  "buildResources" should "create CheckerResources from factory functions" in {
    val logger     = new StubPipelineLogger
    val httpClient = Client.fromHttpApp[IO](org.http4s.HttpApp.notFound[IO])

    val resources = BillTextCheckerPipeline
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

    val _ = resources
  }

  it should "pass correct config sections to each factory" in {
    val logger = new StubPipelineLogger

    val capturedDbConfig =
      new java.util.concurrent.atomic.AtomicReference[DatabaseConfig](
        DatabaseConfig("", 0, "", "", "", 0)
      )
    val capturedPublisherConfig =
      new java.util.concurrent.atomic.AtomicReference[EventPublisherConfig](
        EventPublisherConfig("", "", "")
      )

    val httpClient = Client.fromHttpApp[IO](org.http4s.HttpApp.notFound[IO])

    val _ = BillTextCheckerPipeline
      .buildResources[IO](
        config = testConfig,
        logger = logger,
        transactorFactory = (dbCfg: DatabaseConfig) => {
          capturedDbConfig.set(dbCfg)
          Resource.pure[IO, Transactor[IO]](testXa)
        },
        httpClientFactory = Resource.pure[IO, Client[IO]](httpClient),
        pubSubPublisherFactory = (pubCfg: EventPublisherConfig) => {
          capturedPublisherConfig.set(pubCfg)
          Resource.pure[IO, PubSubEventPublisher[IO]](stubPubSub)
        },
      )
      .use(_ => IO.unit)
      .unsafeRunSync()

    val _ = capturedDbConfig.get().host shouldBe "localhost"
    val _ = capturedDbConfig.get().port shouldBe 5432
    val _ = capturedDbConfig.get().database shouldBe "repcheck_test"

    val _ = capturedPublisherConfig.get().projectId shouldBe "repcheck-test"
    capturedPublisherConfig.get().topicName shouldBe "test-bill-events"
  }

  "buildStream" should "delegate to checker.checkAll and return its results" in {
    val logger         = new StubPipelineLogger
    val checker        = mock[BillTextAvailabilityChecker[IO]]
    val expectedResult = ProcessingResult.Succeeded(entityId = "118-HR-1")

    when(checker.checkAll(anyLong())).thenReturn(Stream.emit(expectedResult))

    val results = BillTextCheckerPipeline.buildStream[IO](checker, logger).compile.toList.unsafeRunSync()

    val _ = results.size shouldBe 1
    results.headOption.map(_.entityId) shouldBe Some("118-HR-1")
  }

  it should "return empty stream when checker produces no results" in {
    val logger  = new StubPipelineLogger
    val checker = mock[BillTextAvailabilityChecker[IO]]

    when(checker.checkAll(anyLong())).thenReturn(Stream.empty)

    val results = BillTextCheckerPipeline.buildStream[IO](checker, logger).compile.toList.unsafeRunSync()

    results shouldBe empty
  }

}
