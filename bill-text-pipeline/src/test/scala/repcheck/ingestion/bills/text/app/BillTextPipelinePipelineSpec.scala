package repcheck.ingestion.bills.text.app

import java.time.Instant
import java.util.UUID

import scala.concurrent.duration._

import cats.effect.unsafe.implicits.global
import cats.effect.{IO, Resource}

import org.http4s.client.Client

import fs2.Stream

import doobie._

import pureconfig.ConfigSource

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar
import repcheck.ingestion.bills.text.app.BillTextPipelinePipeline.{AppConfig, PipelineResources}
import repcheck.ingestion.bills.text.config.BillTextPipelineConfig
import repcheck.ingestion.bills.text.embedding.EmbeddingConfig
import repcheck.ingestion.bills.text.pipeline.BillTextProcessor
import repcheck.ingestion.bills.text.subscription.{EventSubscriberConfig, PubSubEventSubscriber, ReceivedEvent}
import repcheck.ingestion.common.db.DatabaseConfig
import repcheck.ingestion.common.events.{EventPublisherConfig, PubSubEventPublisher}
import repcheck.ingestion.common.execution.PipelineFailureHandlerConfig
import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}
import repcheck.pipeline.models.events.{BillTextAvailableEvent, PipelineEvent}
import repcheck.pipeline.models.metadata.ProcessingResult

class BillTextPipelinePipelineSpec extends AnyFlatSpec with Matchers with MockitoSugar {

  private val testXa: Transactor[IO] = Transactor.fromDriverManager[IO](
    driver = "org.h2.Driver",
    url = "jdbc:h2:mem:pipelinespec;DB_CLOSE_DELAY=-1",
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
    pipeline = BillTextPipelineConfig(
      parallelism = 1,
      downloadTimeoutSeconds = 5,
      maxContentBytes = 1048576L,
      pageDelay = 100.millis,
    ),
    eventPublisher = EventPublisherConfig(
      projectId = "repcheck-test",
      topicName = "test-topic",
      source = "test-source",
    ),
    eventSubscriber = EventSubscriberConfig(
      projectId = "repcheck-test",
      subscriptionId = "test-sub",
      maxMessages = 10,
    ),
    embedding = EmbeddingConfig(
      baseUrl = "http://localhost:11434",
      modelName = "qwen3-embedding",
      dimensions = 1536,
      timeoutSeconds = 5,
    ),
    failureHandler = PipelineFailureHandlerConfig(maxRetries = 1),
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

  private val emptySubscriber: PubSubEventSubscriber[IO] = new PubSubEventSubscriber[IO] {
    def pull(maxMessages: Int): IO[List[ReceivedEvent]] = IO.pure(List.empty)
    def acknowledge(ackIds: List[String]): IO[Unit]     = IO.unit
  }

  private def stubResources(subscriber: PubSubEventSubscriber[IO] = emptySubscriber): PipelineResources[IO] =
    PipelineResources(
      xa = testXa,
      httpClient = Client.fromHttpApp(org.http4s.HttpApp.notFound[IO]),
      pubSubPublisher = stubPubSub,
      pubSubSubscriber = subscriber,
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

  "runWithFactories" should "complete successfully with empty subscriber" in {
    val logger = new StubPipelineLogger

    val exitCode = BillTextPipelinePipeline
      .runWithFactories[IO](
        configLoader = IO.pure(testConfig),
        loggerFactory = (_: String) => IO.pure(logger),
        resourceBuilder =
          (_: AppConfig, _: PipelineLogger[IO]) => Resource.pure[IO, PipelineResources[IO]](stubResources()),
        processorFactory = (_, _, _, _, _) => mock[BillTextProcessor[IO]],
        streamFactory = (_, _, _, _) => Stream.empty,
        workflowStateUpdaterFactory = (_, _) => None,
      )
      .unsafeRunSync()

    exitCode.code shouldBe 0
  }

  it should "propagate config loading failures" in {
    val logger = new StubPipelineLogger

    val result = BillTextPipelinePipeline
      .runWithFactories[IO](
        configLoader = IO.raiseError(new RuntimeException("Config load failed")),
        loggerFactory = (_: String) => IO.pure(logger),
        resourceBuilder =
          (_: AppConfig, _: PipelineLogger[IO]) => Resource.pure[IO, PipelineResources[IO]](stubResources()),
        processorFactory = (_, _, _, _, _) => mock[BillTextProcessor[IO]],
        streamFactory = (_, _, _, _) => Stream.empty,
        workflowStateUpdaterFactory = (_, _) => None,
      )
      .attempt
      .unsafeRunSync()

    val _ = result.isLeft shouldBe true
    result.left.map(_.getMessage) shouldBe Left("Config load failed")
  }

  it should "log pipeline summary after execution" in {
    val logger = new StubPipelineLogger

    val _ = BillTextPipelinePipeline
      .runWithFactories[IO](
        configLoader = IO.pure(testConfig),
        loggerFactory = (_: String) => IO.pure(logger),
        resourceBuilder =
          (_: AppConfig, _: PipelineLogger[IO]) => Resource.pure[IO, PipelineResources[IO]](stubResources()),
        processorFactory = (_, _, _, _, _) => mock[BillTextProcessor[IO]],
        streamFactory = (_, _, _, _) => Stream.empty,
        workflowStateUpdaterFactory = (_, _) => None,
      )
      .unsafeRunSync()

    val summaryLogs = logger.messages.filter(_.contains("Pipeline completed"))
    summaryLogs should not be empty
  }

  "buildProcessor" should "construct a BillTextProcessor with all dependencies" in {
    val logger     = new StubPipelineLogger
    val httpClient = Client.fromHttpApp[IO](org.http4s.HttpApp.notFound[IO])

    val processor = BillTextPipelinePipeline.buildProcessor[IO](
      httpClient,
      testXa,
      stubPubSub,
      testConfig,
      logger,
    )

    processor.toString should not be empty
  }

  "buildStream" should "produce results from subscriber events" in {
    val logger        = new StubPipelineLogger
    val testEventId   = UUID.randomUUID()
    val correlationId = UUID.randomUUID()

    val testEvent = BillTextAvailableEvent(
      naturalKey = "118-HR-1",
      congress = 118,
      textUrl = "https://example.com/text",
      textFormat = "Formatted Text",
      versionCode = "IH",
      previousVersionCode = None,
    )

    val pipelineEvent = PipelineEvent(
      eventType = "bill.text.available",
      payload = testEvent,
      timestamp = Instant.now(),
      eventId = testEventId,
      correlationId = correlationId,
      source = "test",
    )

    val subscriber: PubSubEventSubscriber[IO] = new PubSubEventSubscriber[IO] {
      def pull(maxMessages: Int): IO[List[ReceivedEvent]] =
        IO.pure(List(ReceivedEvent(pipelineEvent, "ack-1")))
      def acknowledge(ackIds: List[String]): IO[Unit] = IO.unit
    }

    val processor = mock[BillTextProcessor[IO]]
    org.mockito.Mockito
      .when(
        processor.processEvent(
          org.mockito.ArgumentMatchers.any[BillTextAvailableEvent],
          org.mockito.ArgumentMatchers.any[UUID],
        )
      )
      .thenReturn(IO.pure(ProcessingResult.Succeeded("118-HR-1")))

    val stream  = BillTextPipelinePipeline.buildStream[IO](subscriber, processor, testConfig, logger)
    val results = stream.compile.toList.unsafeRunSync()

    val _ = results.size shouldBe 1
    results.headOption.map(_.isSucceeded) shouldBe Some(true)
  }

  it should "produce empty stream when subscriber has no messages" in {
    val logger    = new StubPipelineLogger
    val processor = mock[BillTextProcessor[IO]]

    val stream  = BillTextPipelinePipeline.buildStream[IO](emptySubscriber, processor, testConfig, logger)
    val results = stream.compile.toList.unsafeRunSync()

    results shouldBe empty
  }

  "processAndAck" should "acknowledge successfully processed events" in {
    val logger        = new StubPipelineLogger
    val ackedIds      = new java.util.concurrent.atomic.AtomicReference[List[String]](List.empty)
    val correlationId = UUID.randomUUID()

    val subscriber: PubSubEventSubscriber[IO] = new PubSubEventSubscriber[IO] {
      def pull(maxMessages: Int): IO[List[ReceivedEvent]] = IO.pure(List.empty)
      def acknowledge(ackIds: List[String]): IO[Unit] = IO {
        val _ = ackedIds.updateAndGet(ids => ids ++ ackIds)
      }
    }

    val testEvent = BillTextAvailableEvent(
      naturalKey = "118-HR-1",
      congress = 118,
      textUrl = "https://example.com/text",
      textFormat = "Formatted Text",
      versionCode = "IH",
      previousVersionCode = None,
    )

    val pipelineEvent = PipelineEvent(
      eventType = "bill.text.available",
      payload = testEvent,
      timestamp = Instant.now(),
      eventId = UUID.randomUUID(),
      correlationId = correlationId,
      source = "test",
    )

    val processor = mock[BillTextProcessor[IO]]
    org.mockito.Mockito
      .when(
        processor.processEvent(
          org.mockito.ArgumentMatchers.any[BillTextAvailableEvent],
          org.mockito.ArgumentMatchers.any[UUID],
        )
      )
      .thenReturn(IO.pure(ProcessingResult.Succeeded("118-HR-1")))

    val result = BillTextPipelinePipeline
      .processAndAck[IO](subscriber, processor, ReceivedEvent(pipelineEvent, "ack-42"), logger)
      .unsafeRunSync()

    val _ = result.isSucceeded shouldBe true
    ackedIds.get() shouldBe List("ack-42")
  }

  it should "not acknowledge failed processing results" in {
    val logger        = new StubPipelineLogger
    val ackedIds      = new java.util.concurrent.atomic.AtomicReference[List[String]](List.empty)
    val correlationId = UUID.randomUUID()

    val subscriber: PubSubEventSubscriber[IO] = new PubSubEventSubscriber[IO] {
      def pull(maxMessages: Int): IO[List[ReceivedEvent]] = IO.pure(List.empty)
      def acknowledge(ackIds: List[String]): IO[Unit] = IO {
        val _ = ackedIds.updateAndGet(ids => ids ++ ackIds)
      }
    }

    val testEvent = BillTextAvailableEvent(
      naturalKey = "118-HR-1",
      congress = 118,
      textUrl = "https://example.com/text",
      textFormat = "Formatted Text",
      versionCode = "IH",
      previousVersionCode = None,
    )

    val pipelineEvent = PipelineEvent(
      eventType = "bill.text.available",
      payload = testEvent,
      timestamp = Instant.now(),
      eventId = UUID.randomUUID(),
      correlationId = correlationId,
      source = "test",
    )

    val processor = mock[BillTextProcessor[IO]]
    org.mockito.Mockito
      .when(
        processor.processEvent(
          org.mockito.ArgumentMatchers.any[BillTextAvailableEvent],
          org.mockito.ArgumentMatchers.any[UUID],
        )
      )
      .thenReturn(IO.pure(ProcessingResult.Failed("118-HR-1", "download error")))

    val result = BillTextPipelinePipeline
      .processAndAck[IO](subscriber, processor, ReceivedEvent(pipelineEvent, "ack-99"), logger)
      .unsafeRunSync()

    val _ = result.isFailed shouldBe true
    ackedIds.get() shouldBe empty
  }

  it should "extract correlationId from the PipelineEvent envelope" in {
    val logger        = new StubPipelineLogger
    val correlationId = UUID.fromString("11111111-2222-3333-4444-555555555555")

    val testEvent = BillTextAvailableEvent(
      naturalKey = "118-HR-1",
      congress = 118,
      textUrl = "https://example.com/text",
      textFormat = "Formatted Text",
      versionCode = "IH",
      previousVersionCode = None,
    )

    val pipelineEvent = PipelineEvent(
      eventType = "bill.text.available",
      payload = testEvent,
      timestamp = Instant.now(),
      eventId = UUID.randomUUID(),
      correlationId = correlationId,
      source = "test",
    )

    val capturedCorrelationId = new java.util.concurrent.atomic.AtomicReference[UUID](UUID.randomUUID())

    val processor = mock[BillTextProcessor[IO]]
    org.mockito.Mockito
      .when(
        processor.processEvent(
          org.mockito.ArgumentMatchers.any[BillTextAvailableEvent],
          org.mockito.ArgumentMatchers.any[UUID],
        )
      )
      .thenAnswer { invocation =>
        val cid = invocation.getArgument[UUID](1)
        capturedCorrelationId.set(cid)
        IO.pure(ProcessingResult.Succeeded("118-HR-1"))
      }

    val _ = BillTextPipelinePipeline
      .processAndAck[IO](emptySubscriber, processor, ReceivedEvent(pipelineEvent, "ack-1"), logger)
      .unsafeRunSync()

    capturedCorrelationId.get() shouldBe correlationId
  }

  // --- buildResources tests ---

  "buildResources" should "create PipelineResources from factory functions" in {
    val logger     = new StubPipelineLogger
    val httpClient = Client.fromHttpApp[IO](org.http4s.HttpApp.notFound[IO])

    val resources = BillTextPipelinePipeline
      .buildResources[IO](
        config = testConfig,
        logger = logger,
        transactorFactory = (_: DatabaseConfig) => Resource.pure[IO, Transactor[IO]](testXa),
        httpClientFactory = Resource.pure[IO, Client[IO]](httpClient),
        pubSubPublisherFactory = (_: EventPublisherConfig) => Resource.pure[IO, PubSubEventPublisher[IO]](stubPubSub),
        pubSubSubscriberFactory = (_: EventSubscriberConfig, _: PipelineLogger[IO]) =>
          Resource.pure[IO, PubSubEventSubscriber[IO]](emptySubscriber),
      )
      .use { res =>
        IO {
          val _ = res.xa.toString should not be empty
          val _ = res.httpClient.toString should not be empty
          val _ = res.pubSubPublisher.toString should not be empty
          res.pubSubSubscriber.toString should not be empty
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
    val capturedSubscriberConfig =
      new java.util.concurrent.atomic.AtomicReference[EventSubscriberConfig](
        EventSubscriberConfig("", "", 0)
      )

    val httpClient = Client.fromHttpApp[IO](org.http4s.HttpApp.notFound[IO])

    val _ = BillTextPipelinePipeline
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
        pubSubSubscriberFactory = (subCfg: EventSubscriberConfig, _: PipelineLogger[IO]) => {
          capturedSubscriberConfig.set(subCfg)
          Resource.pure[IO, PubSubEventSubscriber[IO]](emptySubscriber)
        },
      )
      .use(_ => IO.unit)
      .unsafeRunSync()

    val _ = capturedDbConfig.get().host shouldBe "localhost"
    val _ = capturedDbConfig.get().port shouldBe 5432
    val _ = capturedDbConfig.get().database shouldBe "repcheck_test"

    val _ = capturedPublisherConfig.get().projectId shouldBe "repcheck-test"
    val _ = capturedPublisherConfig.get().topicName shouldBe "test-topic"

    val _ = capturedSubscriberConfig.get().projectId shouldBe "repcheck-test"
    capturedSubscriberConfig.get().subscriptionId shouldBe "test-sub"
  }

}
