package com.repcheck.bills.text.app

import java.util.UUID

import cats.effect.unsafe.implicits.global
import cats.effect.{IO, Resource}

import org.http4s.client.Client

import doobie._

import pureconfig.ConfigSource

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar
import repcheck.ingestion.common.db.DatabaseConfig
import repcheck.ingestion.common.events.{EventPublisherConfig, PubSubEventPublisher}
import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}

import com.repcheck.bills.text.app.BillTextPipelinePipeline.AppConfig
import com.repcheck.bills.text.config.BillTextPipelineConfig
import com.repcheck.bills.text.pipeline.BillTextProcessor

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
    ),
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
    val result = ConfigSource
      .resources("application-test.conf")
      .withFallback(ConfigSource.resources("application.conf"))
      .load[AppConfig]
    val _ = withClue(s"Config load failed: ${result.left.map(_.prettyPrint(0))}")(
      result.isRight shouldBe true
    )
  }

  "runWithFactories" should "complete successfully with empty event stream" in {
    val logger = new StubPipelineLogger

    val exitCode = BillTextPipelinePipeline
      .runWithFactories[IO](
        configLoader = IO.pure(testConfig),
        loggerFactory = (_: String) => IO.pure(logger),
        resourceBuilder = (_: AppConfig) =>
          Resource.pure[IO, (Transactor[IO], Client[IO], PubSubEventPublisher[IO])](
            (testXa, Client.fromHttpApp(org.http4s.HttpApp.notFound[IO]), stubPubSub)
          ),
        processorFactory = (_, _, _, _, _) => mock[BillTextProcessor[IO]],
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
        resourceBuilder = (_: AppConfig) =>
          Resource.pure[IO, (Transactor[IO], Client[IO], PubSubEventPublisher[IO])](
            (testXa, Client.fromHttpApp(org.http4s.HttpApp.notFound[IO]), stubPubSub)
          ),
        processorFactory = (_, _, _, _, _) => mock[BillTextProcessor[IO]],
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
        resourceBuilder = (_: AppConfig) =>
          Resource.pure[IO, (Transactor[IO], Client[IO], PubSubEventPublisher[IO])](
            (testXa, Client.fromHttpApp(org.http4s.HttpApp.notFound[IO]), stubPubSub)
          ),
        processorFactory = (_, _, _, _, _) => mock[BillTextProcessor[IO]],
      )
      .unsafeRunSync()

    val summaryLogs = logger.messages.filter(_.contains("Pipeline completed"))
    summaryLogs should not be empty
  }

}
