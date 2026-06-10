package repcheck.ingestion.members.profile.app

import java.util.UUID

import scala.concurrent.duration._

import cats.effect.unsafe.implicits.global
import cats.effect.{IO, Resource}

import org.http4s.client.Client

import fs2.Stream

import doobie._

import pureconfig.ConfigSource

import org.mockito.ArgumentMatchers.{anyLong, eq => eqTo}
import org.mockito.Mockito.{times, verify, when}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar
import repcheck.ingestion.common.api.CongressGovClientConfig
import repcheck.ingestion.common.db.DatabaseConfig
import repcheck.ingestion.common.events.{EventPublisherConfig, PubSubEventPublisher}
import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}
import repcheck.ingestion.members.profile.app.MemberProfilePipeline.{AppConfig, PipelineResources}
import repcheck.ingestion.members.profile.config.MemberProfileConfig
import repcheck.ingestion.members.profile.pipeline.MemberProfileProcessor
import repcheck.pipeline.models.metadata.ProcessingResult

import com.repcheck.utils.errors.{ErrorClass, RetryConfig}

class MemberProfilePipelineSpec extends AnyFlatSpec with Matchers with MockitoSugar {

  private val testXa: Transactor[IO] = Transactor.fromDriverManager[IO](
    driver = "org.h2.Driver",
    url = "jdbc:h2:mem:memberprofilepipelinespec;DB_CLOSE_DELAY=-1",
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
    congressApi = CongressGovClientConfig(
      apiKey = "test-api-key",
      baseUrl = "http://localhost:8080",
      retry = testRetry,
    ),
    pipeline = MemberProfileConfig(
      congresses = List(118),
      parallelism = 1,
      pageDelay = 0.millis,
      eventPublishRetry = testRetry,
    ),
    eventPublisher = EventPublisherConfig(
      projectId = "repcheck-test",
      topicName = "test-member-events",
      source = "member-profile-pipeline-test",
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

  private def stubResources(): PipelineResources[IO] =
    PipelineResources(
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

  // Stub resolver shared by tests that don't care about congress resolution (it always returns the test list).
  private val stubCongressesResolver: (AppConfig, Transactor[IO], PipelineLogger[IO]) => IO[List[Int]] =
    (_, _, _) => IO.pure(List(118))

  "runWithFactories" should "complete successfully with empty stream" in {
    val logger = new StubPipelineLogger

    val exitCode = MemberProfilePipeline
      .runWithFactories[IO](
        configLoader = IO.pure(testConfig),
        loggerFactory = (_: String) => IO.pure(logger),
        resourceBuilder =
          (_: AppConfig, _: PipelineLogger[IO]) => Resource.pure[IO, PipelineResources[IO]](stubResources()),
        processorFactory = (_, _, _, _, _) => mock[MemberProfileProcessor[IO]],
        congressesResolver = stubCongressesResolver,
        streamFactory = (_, _, _) => Stream.empty,
      )
      .unsafeRunSync()

    exitCode.code shouldBe 0
  }

  it should "return ExitCode.Error when the stream has failures" in {
    val logger = new StubPipelineLogger

    val exitCode = MemberProfilePipeline
      .runWithFactories[IO](
        configLoader = IO.pure(testConfig),
        loggerFactory = (_: String) => IO.pure(logger),
        resourceBuilder =
          (_: AppConfig, _: PipelineLogger[IO]) => Resource.pure[IO, PipelineResources[IO]](stubResources()),
        processorFactory = (_, _, _, _, _) => mock[MemberProfileProcessor[IO]],
        congressesResolver = stubCongressesResolver,
        streamFactory = (_, _, _) => Stream.emit(ProcessingResult.Failed("A000001", "api error")),
      )
      .unsafeRunSync()

    exitCode.code shouldBe 1
  }

  it should "propagate config loading failures" in {
    val logger = new StubPipelineLogger

    val result = MemberProfilePipeline
      .runWithFactories[IO](
        configLoader = IO.raiseError(new RuntimeException("Config load failed")),
        loggerFactory = (_: String) => IO.pure(logger),
        resourceBuilder =
          (_: AppConfig, _: PipelineLogger[IO]) => Resource.pure[IO, PipelineResources[IO]](stubResources()),
        processorFactory = (_, _, _, _, _) => mock[MemberProfileProcessor[IO]],
        congressesResolver = stubCongressesResolver,
        streamFactory = (_, _, _) => Stream.empty,
      )
      .attempt
      .unsafeRunSync()

    val _ = result.isLeft shouldBe true
    result.left.map(_.getMessage) shouldBe Left("Config load failed")
  }

  it should "log pipeline summary after execution" in {
    val logger = new StubPipelineLogger

    val _ = MemberProfilePipeline
      .runWithFactories[IO](
        configLoader = IO.pure(testConfig),
        loggerFactory = (_: String) => IO.pure(logger),
        resourceBuilder =
          (_: AppConfig, _: PipelineLogger[IO]) => Resource.pure[IO, PipelineResources[IO]](stubResources()),
        processorFactory = (_, _, _, _, _) => mock[MemberProfileProcessor[IO]],
        congressesResolver = stubCongressesResolver,
        streamFactory = (_, _, _) => Stream.empty,
      )
      .unsafeRunSync()

    val summaryLogs = logger.messages.filter(_.contains("Pipeline completed"))
    summaryLogs should not be empty
  }

  it should "thread the resolved congresses list through to the stream factory" in {
    val logger       = new StubPipelineLogger
    val capturedList = new java.util.concurrent.atomic.AtomicReference[List[Int]](Nil)
    val resolverList = List(116, 117, 118, 119)

    val _ = MemberProfilePipeline
      .runWithFactories[IO](
        configLoader = IO.pure(testConfig),
        loggerFactory = (_: String) => IO.pure(logger),
        resourceBuilder =
          (_: AppConfig, _: PipelineLogger[IO]) => Resource.pure[IO, PipelineResources[IO]](stubResources()),
        processorFactory = (_, _, _, _, _) => mock[MemberProfileProcessor[IO]],
        congressesResolver = (_, _, _) => IO.pure(resolverList),
        streamFactory = (_, _, congresses) => {
          capturedList.set(congresses)
          Stream.empty
        },
      )
      .unsafeRunSync()

    capturedList.get() shouldBe resolverList
  }

  "noOpRetryLogger" should "return F[Unit] regardless of arguments" in {
    val result = MemberProfilePipeline
      .noOpRetryLogger[IO]
      .apply(1, 3, 100L, ErrorClass.Transient, "test error", UUID.randomUUID())
      .unsafeRunSync()

    result shouldBe ((): Unit)
  }

  "buildProcessor" should "construct a MemberProfileProcessor with all dependencies" in {
    val logger     = new StubPipelineLogger
    val httpClient = Client.fromHttpApp[IO](org.http4s.HttpApp.notFound[IO])

    val processor = MemberProfilePipeline.buildProcessor[IO](
      httpClient,
      testXa,
      stubPubSub,
      testConfig,
      logger,
    )

    processor.toString should not be empty
  }

  "buildStream" should "delegate to processor.streamAll with the supplied congresses list" in {
    val logger    = new StubPipelineLogger
    val processor = mock[MemberProfileProcessor[IO]]

    when(processor.streamAll(anyLong(), eqTo(List(118, 119))))
      .thenReturn(Stream.emit(ProcessingResult.Succeeded("A000001")))

    val results =
      MemberProfilePipeline.buildStream[IO](processor, logger, List(118, 119)).compile.toList.unsafeRunSync()

    val _ = results.size shouldBe 1
    val _ = results.headOption.map(_.entityId) shouldBe Some("A000001")
    verify(processor, times(1)).streamAll(anyLong(), eqTo(List(118, 119)))
  }

  it should "return empty stream when processor produces no results" in {
    val logger    = new StubPipelineLogger
    val processor = mock[MemberProfileProcessor[IO]]

    when(processor.streamAll(anyLong(), eqTo(List(118)))).thenReturn(Stream.empty)

    val results = MemberProfilePipeline.buildStream[IO](processor, logger, List(118)).compile.toList.unsafeRunSync()

    results shouldBe empty
  }

  "buildResources" should "create PipelineResources from factory functions" in {
    val logger     = new StubPipelineLogger
    val httpClient = Client.fromHttpApp[IO](org.http4s.HttpApp.notFound[IO])

    val resources = MemberProfilePipeline
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

    val _ = MemberProfilePipeline
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
    capturedPublisherConfig.get().topicName shouldBe "test-member-events"
  }

}
