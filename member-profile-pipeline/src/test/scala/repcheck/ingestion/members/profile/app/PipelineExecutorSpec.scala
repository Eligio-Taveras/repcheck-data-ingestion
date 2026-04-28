package repcheck.ingestion.members.profile.app

import cats.effect.IO
import cats.effect.unsafe.implicits.global

import fs2.Stream

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}
import repcheck.pipeline.models.metadata.ProcessingResult

class PipelineExecutorSpec extends AnyFlatSpec with Matchers {

  private val runId        = 12345L
  private val pipelineName = "test-pipeline"

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

  "execute" should "return ExitCode.Success when all results succeed" in {
    val logger = new StubPipelineLogger
    val stream = Stream.emits(
      List(
        ProcessingResult.Succeeded("member-A000001"),
        ProcessingResult.Succeeded("member-A000002"),
      )
    )

    val result = PipelineExecutor.execute[IO](stream, logger, pipelineName, runId).unsafeRunSync()
    result.code shouldBe 0
  }

  it should "return ExitCode.Error when any result fails" in {
    val logger = new StubPipelineLogger
    val stream = Stream.emits(
      List(
        ProcessingResult.Succeeded("member-A000001"),
        ProcessingResult.Failed("member-A000002", "conversion error"),
      )
    )

    val result = PipelineExecutor.execute[IO](stream, logger, pipelineName, runId).unsafeRunSync()
    result.code shouldBe 1
  }

  it should "return ExitCode.Success when all results are skipped" in {
    val logger = new StubPipelineLogger
    val stream = Stream.emits(
      List(
        ProcessingResult.Skipped("member-A000001", "unchanged"),
        ProcessingResult.Skipped("member-A000002", "unchanged"),
      )
    )

    val result = PipelineExecutor.execute[IO](stream, logger, pipelineName, runId).unsafeRunSync()
    result.code shouldBe 0
  }

  it should "return ExitCode.Success for an empty result stream" in {
    val logger                               = new StubPipelineLogger
    val stream: Stream[IO, ProcessingResult] = Stream.empty

    val result = PipelineExecutor.execute[IO](stream, logger, pipelineName, runId).unsafeRunSync()
    result.code shouldBe 0
  }

  it should "log the pipeline summary with correct counts" in {
    val logger = new StubPipelineLogger
    val stream = Stream.emits(
      List(
        ProcessingResult.Succeeded("member-A000001"),
        ProcessingResult.Succeeded("member-A000002"),
        ProcessingResult.Failed("member-A000003", "api error"),
        ProcessingResult.Skipped("member-A000004", "unchanged"),
      )
    )

    val _ = PipelineExecutor.execute[IO](stream, logger, pipelineName, runId).unsafeRunSync()

    val _          = logger.messages.size shouldBe 1
    val logMessage = logger.messages.headOption.getOrElse(fail("expected at least one log message"))
    val _          = logMessage should include("4 processed")
    // Skipped results count toward succeeded (per pipeline-models v0.1.21 — idempotent skip is a successful no-op).
    val _ = logMessage should include("3 succeeded")
    logMessage should include("1 failed")
  }

  it should "return ExitCode.Error when all results fail" in {
    val logger = new StubPipelineLogger
    val stream = Stream.emits(
      List(
        ProcessingResult.Failed("member-A000001", "error 1"),
        ProcessingResult.Failed("member-A000002", "error 2"),
      )
    )

    val result = PipelineExecutor.execute[IO](stream, logger, pipelineName, runId).unsafeRunSync()
    result.code shouldBe 1
  }

  it should "return ExitCode.Success for mixed succeeded and skipped results with zero failures" in {
    val logger = new StubPipelineLogger
    val stream = Stream.emits(
      List(
        ProcessingResult.Succeeded("member-A000001"),
        ProcessingResult.Skipped("member-A000002", "unchanged"),
        ProcessingResult.Succeeded("member-A000003"),
      )
    )

    val result = PipelineExecutor.execute[IO](stream, logger, pipelineName, runId).unsafeRunSync()
    result.code shouldBe 0
  }

  it should "handle a stream that raises an error" in {
    val logger = new StubPipelineLogger
    val stream: Stream[IO, ProcessingResult] =
      Stream.emit(ProcessingResult.Succeeded("member-A000001")) ++
        Stream.raiseError[IO](new RuntimeException("stream failure"))

    val result = PipelineExecutor.execute[IO](stream, logger, pipelineName, runId).attempt.unsafeRunSync()
    result.isLeft shouldBe true
  }

}
