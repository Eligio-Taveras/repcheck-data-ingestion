package repcheck.members.lismapping.app

import java.util.UUID

import scala.concurrent.duration._

import cats.effect.unsafe.implicits.global
import cats.effect.{IO, Resource}

import org.http4s.client.Client

import doobie._

import pureconfig.ConfigSource

import org.mockito.ArgumentMatchers.anyLong
import org.mockito.Mockito.when
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar
import repcheck.ingestion.common.db.DatabaseConfig
import repcheck.ingestion.common.events.{EventPublisherConfig, PubSubEventPublisher}
import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}
import repcheck.members.lismapping.app.LisMappingRefresherPipeline.{AppConfig, RefresherResources}
import repcheck.members.lismapping.config.LisMappingConfig
import repcheck.members.lismapping.pipeline.{LisMappingProcessor, LisMappingRefreshResult}

import com.repcheck.utils.errors.{ErrorClass, RetryConfig}

class LisMappingRefresherPipelineSpec extends AnyFlatSpec with Matchers with MockitoSugar {

  private val testXa: Transactor[IO] = Transactor.fromDriverManager[IO](
    driver = "org.h2.Driver",
    url = "jdbc:h2:mem:lismappingrefresherspec;DB_CLOSE_DELAY=-1",
    user = "",
    password = "",
    logHandler = None,
  )

  private val testRetry = RetryConfig(
    maxRetries = 0,
    initialBackoffMs = 1,
    maxBackoffMs = 10,
    backoffMultiplier = 1.0,
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
    pipeline = LisMappingConfig(
      parallelism = 1,
      requestTimeout = 5.seconds,
      currentCongress = 118,
      congressLookbackWindow = 5,
      senatorXmlUrl = "http://localhost:9999/senators.xml",
    ),
    fetchRetry = testRetry,
    eventPublisher = EventPublisherConfig(
      projectId = "repcheck-test",
      topicName = "test-member-events",
      source = "lis-mapping-refresher-test",
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

  private def stubResources(): RefresherResources[IO] =
    RefresherResources(
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

  "runWithFactories" should "complete successfully and return ExitCode.Success" in {
    val logger    = new StubPipelineLogger
    val processor = mock[LisMappingProcessor[IO]]

    when(processor.refreshAll(anyLong())).thenReturn(IO.pure(LisMappingRefreshResult.empty))

    val exitCode = LisMappingRefresherPipeline
      .runWithFactories[IO](
        configLoader = IO.pure(testConfig),
        loggerFactory = (_: String) => IO.pure(logger),
        resourceBuilder =
          (_: AppConfig, _: PipelineLogger[IO]) => Resource.pure[IO, RefresherResources[IO]](stubResources()),
        processorFactory = (_, _, _, _, _) => processor,
        runId = 1L,
      )
      .unsafeRunSync()

    exitCode.code shouldBe 0
  }

  it should "propagate config loading failures" in {
    val logger = new StubPipelineLogger

    val result = LisMappingRefresherPipeline
      .runWithFactories[IO](
        configLoader = IO.raiseError(new RuntimeException("Config load failed")),
        loggerFactory = (_: String) => IO.pure(logger),
        resourceBuilder =
          (_: AppConfig, _: PipelineLogger[IO]) => Resource.pure[IO, RefresherResources[IO]](stubResources()),
        processorFactory = (_, _, _, _, _) => mock[LisMappingProcessor[IO]],
        runId = 1L,
      )
      .attempt
      .unsafeRunSync()

    val _ = result.isLeft shouldBe true
    result.left.map(_.getMessage) shouldBe Left("Config load failed")
  }

  it should "propagate processor failures" in {
    val logger    = new StubPipelineLogger
    val processor = mock[LisMappingProcessor[IO]]

    when(processor.refreshAll(anyLong())).thenReturn(
      IO.raiseError(new RuntimeException("XML fetch failed"))
    )

    val result = LisMappingRefresherPipeline
      .runWithFactories[IO](
        configLoader = IO.pure(testConfig),
        loggerFactory = (_: String) => IO.pure(logger),
        resourceBuilder =
          (_: AppConfig, _: PipelineLogger[IO]) => Resource.pure[IO, RefresherResources[IO]](stubResources()),
        processorFactory = (_, _, _, _, _) => processor,
        runId = 1L,
      )
      .attempt
      .unsafeRunSync()

    val _ = result.isLeft shouldBe true
    result.left.map(_.getMessage) shouldBe Left("XML fetch failed")
  }

  it should "log the refresh result summary" in {
    val logger    = new StubPipelineLogger
    val processor = mock[LisMappingProcessor[IO]]

    val result = LisMappingRefreshResult(totalParsed = 100, inserted = 5, updated = 95, eventsEmitted = 5)
    when(processor.refreshAll(anyLong())).thenReturn(IO.pure(result))

    val _ = LisMappingRefresherPipeline
      .runWithFactories[IO](
        configLoader = IO.pure(testConfig),
        loggerFactory = (_: String) => IO.pure(logger),
        resourceBuilder =
          (_: AppConfig, _: PipelineLogger[IO]) => Resource.pure[IO, RefresherResources[IO]](stubResources()),
        processorFactory = (_, _, _, _, _) => processor,
        runId = 1L,
      )
      .unsafeRunSync()

    val summaryLogs = logger.messages.filter(_.contains("Pipeline completed"))
    val _           = summaryLogs should not be empty
    val logMessage  = summaryLogs.headOption.getOrElse(fail("expected summary log"))
    val _           = logMessage should include("totalParsed=100")
    val _           = logMessage should include("inserted=5")
    val _           = logMessage should include("updated=95")
    logMessage should include("eventsEmitted=5")
  }

  "noOpRetryLogger" should "return F[Unit] regardless of arguments" in {
    val result = LisMappingRefresherPipeline
      .noOpRetryLogger[IO]
      .apply(1, 3, 100L, ErrorClass.Transient, "test error", UUID.randomUUID())
      .unsafeRunSync()

    result shouldBe ((): Unit)
  }

  "buildProcessor" should "construct a LisMappingProcessor with all dependencies" in {
    val logger     = new StubPipelineLogger
    val httpClient = Client.fromHttpApp[IO](org.http4s.HttpApp.notFound[IO])

    val processor = LisMappingRefresherPipeline.buildProcessor[IO](
      httpClient,
      testXa,
      stubPubSub,
      testConfig,
      logger,
    )

    processor.toString should not be empty
  }

  "buildResources" should "create RefresherResources from factory functions" in {
    val logger     = new StubPipelineLogger
    val httpClient = Client.fromHttpApp[IO](org.http4s.HttpApp.notFound[IO])

    val resources = LisMappingRefresherPipeline
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

    val _ = LisMappingRefresherPipeline
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
    val _ = capturedDbConfig.get().database shouldBe "repcheck_test"
    val _ = capturedPublisherConfig.get().projectId shouldBe "repcheck-test"
    capturedPublisherConfig.get().topicName shouldBe "test-member-events"
  }

}
