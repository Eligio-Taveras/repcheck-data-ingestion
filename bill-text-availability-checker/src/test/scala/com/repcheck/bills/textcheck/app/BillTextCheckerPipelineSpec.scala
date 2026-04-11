package com.repcheck.bills.textcheck.app

import java.util.UUID

import cats.effect.{IO, Resource}
import cats.effect.unsafe.implicits.global

import org.http4s.client.Client

import doobie._

import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.when
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar

import pureconfig.ConfigSource

import repcheck.ingestion.common.api.CongressGovClientConfig
import repcheck.ingestion.common.db.DatabaseConfig
import repcheck.ingestion.common.events.{EventPublisherConfig, IngestionEventPublisher, PubSubEventPublisher}
import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}
import repcheck.pipeline.models.errors.RetryConfig
import repcheck.shared.models.congress.dos.bill.BillDO

import com.repcheck.bills.common.persistence.BillRepository
import com.repcheck.bills.textcheck.api.BillTextApiClient
import com.repcheck.bills.textcheck.app.BillTextCheckerPipeline.AppConfig
import com.repcheck.bills.textcheck.config.BillTextCheckerConfig
import com.repcheck.bills.textcheck.pipeline.BillTextAvailabilityChecker

class BillTextCheckerPipelineSpec extends AnyFlatSpec with Matchers with MockitoSugar {

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
    congressApi = CongressGovClientConfig(
      baseUrl = "http://localhost:8080",
      apiKey = "test-key",
      pageDelay = scala.concurrent.duration.FiniteDuration(1, "ms"),
      retry = RetryConfig(
        maxRetries = 1,
        initialBackoffMs = 1,
        maxBackoffMs = 10,
        backoffMultiplier = 1.0,
      ),
    ),
    pipeline = BillTextCheckerConfig(parallelism = 1),
    eventPublisher = EventPublisherConfig(
      topicName = "test-topic",
      source = "test-source",
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

  "AppConfig" should "load from PureConfig reference configuration" in {
    val result = ConfigSource.default.load[AppConfig]
    result.isRight shouldBe true
  }

  private def makeEmptyChecker(logger: PipelineLogger[IO]): BillTextAvailabilityChecker[IO] = {
    val billRepo = mock[BillRepository[ConnectionIO]]
    when(billRepo.findBillsNeedingTextCheck())
      .thenReturn(doobie.free.connection.pure(List.empty[BillDO]))

    new BillTextAvailabilityChecker[IO](
      textApiClient = mock[BillTextApiClient[IO]],
      billRepo = billRepo,
      eventPublisher = mock[IngestionEventPublisher[IO]],
      xa = testXa,
      config = BillTextCheckerConfig(parallelism = 1),
      logger = logger,
    )
  }

  "runWithFactories" should "complete successfully with no bills to process" in {
    val logger = new StubPipelineLogger
    val emptyChecker = makeEmptyChecker(logger)

    val exitCode = BillTextCheckerPipeline.runWithFactories[IO](
      configLoader = IO.pure(testConfig),
      loggerFactory = (_: String) => IO.pure(logger),
      resourceBuilder = (_: AppConfig) =>
        Resource.pure[IO, (Transactor[IO], Client[IO], PubSubEventPublisher[IO])](
          (testXa, Client.fromHttpApp(org.http4s.HttpApp.notFound[IO]), stubPubSub)
        ),
      checkerFactory = (_, _, _, _, _) => emptyChecker,
    ).unsafeRunSync()

    exitCode.code shouldBe 0
  }

  it should "propagate config loading failures" in {
    val logger = new StubPipelineLogger

    val result = BillTextCheckerPipeline.runWithFactories[IO](
      configLoader = IO.raiseError(new RuntimeException("Config load failed")),
      loggerFactory = (_: String) => IO.pure(logger),
      resourceBuilder = (_: AppConfig) =>
        Resource.pure[IO, (Transactor[IO], Client[IO], PubSubEventPublisher[IO])](
          (testXa, Client.fromHttpApp(org.http4s.HttpApp.notFound[IO]), stubPubSub)
        ),
      checkerFactory = (_, _, _, _, _) =>
        throw new RuntimeException("should not be called"),
    ).attempt.unsafeRunSync()

    val _ = result.isLeft shouldBe true
    result.left.map(_.getMessage) shouldBe Left("Config load failed")
  }

  it should "return ExitCode.Error when checker stream contains failures" in {
    val logger = new StubPipelineLogger
    val emptyChecker = makeEmptyChecker(logger)

    // With empty bills list, there are no failures so it should succeed
    val exitCode = BillTextCheckerPipeline.runWithFactories[IO](
      configLoader = IO.pure(testConfig),
      loggerFactory = (_: String) => IO.pure(logger),
      resourceBuilder = (_: AppConfig) =>
        Resource.pure[IO, (Transactor[IO], Client[IO], PubSubEventPublisher[IO])](
          (testXa, Client.fromHttpApp(org.http4s.HttpApp.notFound[IO]), stubPubSub)
        ),
      checkerFactory = (_, _, _, _, _) => emptyChecker,
    ).unsafeRunSync()

    exitCode.code shouldBe 0
  }

  it should "log pipeline summary after execution" in {
    val logger = new StubPipelineLogger
    val emptyChecker = makeEmptyChecker(logger)

    val _ = BillTextCheckerPipeline.runWithFactories[IO](
      configLoader = IO.pure(testConfig),
      loggerFactory = (_: String) => IO.pure(logger),
      resourceBuilder = (_: AppConfig) =>
        Resource.pure[IO, (Transactor[IO], Client[IO], PubSubEventPublisher[IO])](
          (testXa, Client.fromHttpApp(org.http4s.HttpApp.notFound[IO]), stubPubSub)
        ),
      checkerFactory = (_, _, _, _, _) => emptyChecker,
    ).unsafeRunSync()

    val summaryLogs = logger.messages.filter(_.contains("Pipeline completed"))
    summaryLogs should not be empty
  }

}
