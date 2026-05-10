package repcheck.ingestion.amendments.text.app

import java.time.Instant
import java.util.UUID

import scala.concurrent.duration._

import cats.effect.unsafe.implicits.global
import cats.effect.{IO, Resource}

import org.http4s.client.Client

import fs2.Stream

import doobie._

import org.mockito.ArgumentMatchers.any
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar
import repcheck.ingestion.amendments.text.app.AmendmentTextPipelinePipeline.{AppConfig, PipelineResources}
import repcheck.ingestion.amendments.text.config.AmendmentTextPipelineConfig
import repcheck.ingestion.amendments.text.embedding.CrossAmendmentEmbedder
import repcheck.ingestion.amendments.text.pipeline.AmendmentTextProcessor
import repcheck.ingestion.amendments.text.subscription.{EventSubscriberConfig, PubSubEventSubscriber, ReceivedEvent}
import repcheck.ingestion.common.db.DatabaseConfig
import repcheck.ingestion.common.execution.PipelineFailureHandlerConfig
import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}
import repcheck.ingestion.text.embedding.EmbeddingConfig
import repcheck.pipeline.models.events.{AmendmentTextAvailableEvent, PipelineEvent}
import repcheck.pipeline.models.metadata.ProcessingResult
import repcheck.shared.models.congress.amendment.AmendmentType

class AmendmentTextPipelinePipelineSpec extends AnyFlatSpec with Matchers with MockitoSugar {

  private val testXa: Transactor[IO] = Transactor.fromDriverManager[IO](
    driver = "org.h2.Driver",
    url = "jdbc:h2:mem:amend-pipelinespec;DB_CLOSE_DELAY=-1",
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
    pipeline = AmendmentTextPipelineConfig(
      parallelism = 1,
      pageDelay = 100.millis,
      govInfoApiKey = "test-key",
      govInfoBaseUrl = "https://api.govinfo.gov",
    ), // remaining fields (govInfoPermits, govInfoHttp, ollamaHttp) use case-class defaults
    eventSubscriber = EventSubscriberConfig(
      projectId = "repcheck-test",
      subscriptionId = "test-sub",
      maxMessages = 10,
      pullTimeout = 30.seconds,
    ),
    embedding = EmbeddingConfig(
      baseUrl = "http://localhost:11434",
      modelName = "bill-text-embedding",
      dimensions = 1024,
      timeoutSeconds = 5,
      maxChunkChars = 30000,
      embedBatchSize = 50,
      embedBatchTimeout = scala.concurrent.duration.DurationInt(1).second,
      embedQueueCapacityMultiplier = 10,
    ),
    failureHandler = PipelineFailureHandlerConfig(maxRetries = 1),
  )

  private class StubPipelineLogger extends PipelineLogger[IO] {
    override def info(context: LogContext, message: String): IO[Unit]                            = IO.unit
    override def warn(context: LogContext, message: String): IO[Unit]                            = IO.unit
    override def error(context: LogContext, message: String, cause: Option[Throwable]): IO[Unit] = IO.unit
    override def debug(context: LogContext, message: String): IO[Unit]                           = IO.unit
  }

  private val emptySubscriber: PubSubEventSubscriber[IO] = new PubSubEventSubscriber[IO] {
    def pull(maxMessages: Int): IO[List[ReceivedEvent]] = IO.pure(List.empty)
    def acknowledge(ackIds: List[String]): IO[Unit]     = IO.unit
    def nack(ackIds: List[String]): IO[Unit]            = IO.unit
  }

  private val stubEmbedder: CrossAmendmentEmbedder[IO] = mock[CrossAmendmentEmbedder[IO]]

  private def stubResources(subscriber: PubSubEventSubscriber[IO] = emptySubscriber): PipelineResources[IO] = {
    val noOpClient = Client.fromHttpApp(org.http4s.HttpApp.notFound[IO])
    PipelineResources(
      transactor = testXa,
      govInfoClient = noOpClient,
      ollamaClient = noOpClient,
      pubSubSubscriber = subscriber,
      embedder = stubEmbedder,
    )
  }

  private def buildEvent(naturalKey: String): AmendmentTextAvailableEvent =
    AmendmentTextAvailableEvent(
      amendmentId = 42L,
      naturalKey = naturalKey,
      congress = 117,
      amendmentType = AmendmentType.SAMDT,
      number = "2137",
      versionTypeCode = "SUB",
      formatType = "HTML",
      url = "https://www.congress.gov/117/crec/2021/08/01/167/136/CREC-2021-08-01-pt1-PgS5255.htm",
      publishedDate = Some(Instant.parse("2021-08-01T04:00:00Z")),
      correlationId = UUID.randomUUID(),
    )

  private def envelope(payload: AmendmentTextAvailableEvent): PipelineEvent[AmendmentTextAvailableEvent] =
    PipelineEvent(
      eventType = "amendment.text.available",
      payload = payload,
      timestamp = Instant.now(),
      eventId = UUID.randomUUID(),
      correlationId = payload.correlationId,
      source = "test",
    )

  "runWithFactories" should "exit Success on an empty subscription drain" in {
    val logger    = new StubPipelineLogger
    val processor = mock[AmendmentTextProcessor[IO]]

    val exitCode = AmendmentTextPipelinePipeline
      .runWithFactories[IO](
        configLoader = IO.pure(testConfig),
        loggerFactory = _ => IO.pure(logger),
        resourceBuilder = (_, _) => Resource.pure[IO, PipelineResources[IO]](stubResources()),
        processorFactory = (_, _, _, _, _) => processor,
        streamFactory = (_, _, _, _, _) => Stream.empty,
        workflowStateUpdaterFactory = (_, _) => None,
      )
      .unsafeRunSync()

    exitCode.code shouldBe 0
  }

  it should "exit Error when the result stream contains a Failed result" in {
    val logger    = new StubPipelineLogger
    val processor = mock[AmendmentTextProcessor[IO]]

    val exitCode = AmendmentTextPipelinePipeline
      .runWithFactories[IO](
        configLoader = IO.pure(testConfig),
        loggerFactory = _ => IO.pure(logger),
        resourceBuilder = (_, _) => Resource.pure[IO, PipelineResources[IO]](stubResources()),
        processorFactory = (_, _, _, _, _) => processor,
        streamFactory = (_, _, _, _, _) => Stream.emit(ProcessingResult.Failed("a", "boom", "Systemic")),
        workflowStateUpdaterFactory = (_, _) => None,
      )
      .unsafeRunSync()

    exitCode.code shouldBe 1
  }

  "buildStream" should "drive the processor through one event and ack it on success" in {
    val logger    = new StubPipelineLogger
    val processor = mock[AmendmentTextProcessor[IO]]
    val event     = buildEvent("117-SAMDT-1")

    org.mockito.Mockito
      .when(processor.processEvent(any[AmendmentTextAvailableEvent], any[String], any[IO[Unit]], any[IO[Unit]]))
      .thenReturn(IO.pure(ProcessingResult.Succeeded(event.naturalKey, eventEmitted = false)))

    val pulledRef = new java.util.concurrent.atomic.AtomicBoolean(false)
    val subscriber: PubSubEventSubscriber[IO] = new PubSubEventSubscriber[IO] {
      private val pendingRef = new java.util.concurrent.atomic.AtomicReference[List[ReceivedEvent]](
        List(ReceivedEvent(envelope(event), "ack-1"))
      )
      def pull(maxMessages: Int): IO[List[ReceivedEvent]] = IO {
        pulledRef.set(true)
        pendingRef.getAndSet(List.empty)
      }
      def acknowledge(ackIds: List[String]): IO[Unit] = IO.unit
      def nack(ackIds: List[String]): IO[Unit]        = IO.unit
    }

    val result = AmendmentTextPipelinePipeline
      .buildStream[IO](subscriber, processor, testConfig, logger, runId = 0L)
      .compile
      .toList
      .unsafeRunSync()

    val _ = pulledRef.get() shouldBe true
    val _ = result should have size 1
    result.headOption.map(_.isSucceeded) shouldBe Some(true)
  }

  "buildProcessor" should "construct an AmendmentTextProcessor wired with real Doobie repositories" in {
    val logger     = new StubPipelineLogger
    val noOpClient = Client.fromHttpApp(org.http4s.HttpApp.notFound[IO])
    val processor =
      AmendmentTextPipelinePipeline.buildProcessor[IO](noOpClient, testXa, stubEmbedder, testConfig, logger)
    // The factory wires real Doobie repos; we just need it to materialize without throwing.
    processor shouldBe a[AmendmentTextProcessor[?]]
  }

  "extractTextFn" should "dispatch HTML through the CrecHtmlExtractor" in {
    val bytes = Stream.emits("<html><body><pre>amendment</pre></body></html>".getBytes("UTF-8")).covary[IO]
    val text =
      AmendmentTextPipelinePipeline
        .extractTextFn[IO]
        .apply(bytes, "HTML", "117-SAMDT-2137")
        .compile
        .toList
        .unsafeRunSync()
        .mkString(" ")
    text should include("amendment")
  }

  "buildResources" should "compose the resource graph from supplied factories without raising" in {
    val logger     = new StubPipelineLogger
    val noOpClient = Client.fromHttpApp(org.http4s.HttpApp.notFound[IO])
    val resourceProgram = AmendmentTextPipelinePipeline
      .buildResources[IO](
        config = testConfig,
        logger = logger,
        transactorFactory = _ => Resource.pure[IO, doobie.util.transactor.Transactor[IO]](testXa),
        govInfoClientFactory = Resource.pure[IO, Client[IO]](noOpClient),
        ollamaClientFactory = Resource.pure[IO, Client[IO]](noOpClient),
        pubSubSubscriberFactory = (_, _) => Resource.pure[IO, PubSubEventSubscriber[IO]](emptySubscriber),
      )
      .use { res =>
        IO.pure {
          val _ = res.govInfoClient
          val _ = res.ollamaClient
          val _ = res.embedder
          res.pubSubSubscriber
        }
      }

    val sub = resourceProgram.unsafeRunSync()
    sub shouldBe emptySubscriber
  }

  it should "treat a pull timeout as subscription drained" in {
    val logger    = new StubPipelineLogger
    val processor = mock[AmendmentTextProcessor[IO]]
    val subscriber: PubSubEventSubscriber[IO] = new PubSubEventSubscriber[IO] {
      def pull(maxMessages: Int): IO[List[ReceivedEvent]] = IO.never
      def acknowledge(ackIds: List[String]): IO[Unit]     = IO.unit
      def nack(ackIds: List[String]): IO[Unit]            = IO.unit
    }

    val cfg = testConfig.copy(eventSubscriber = testConfig.eventSubscriber.copy(pullTimeout = 100.millis))

    val result = AmendmentTextPipelinePipeline
      .buildStream[IO](subscriber, processor, cfg, logger, runId = 99L)
      .compile
      .toList
      .unsafeRunSync()

    result shouldBe empty
  }

}
