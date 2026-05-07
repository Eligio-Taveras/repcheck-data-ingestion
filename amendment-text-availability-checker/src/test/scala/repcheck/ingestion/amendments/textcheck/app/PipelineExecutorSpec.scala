package repcheck.ingestion.amendments.textcheck.app

import cats.effect.IO
import cats.effect.unsafe.implicits.global

import fs2.Stream

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}
import repcheck.pipeline.models.metadata.ProcessingResult

class PipelineExecutorSpec extends AnyFlatSpec with Matchers {

  private val runId        = 12345L
  private val pipelineName = "amendment-text-availability-checker"

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
        ProcessingResult.Succeeded("117-SAMDT-1"),
        ProcessingResult.Succeeded("117-SAMDT-2"),
      )
    )
    val result = PipelineExecutor.execute[IO](stream, logger, pipelineName, runId).unsafeRunSync()
    result.code shouldBe 0
  }

  it should "return ExitCode.Error when any result fails" in {
    val logger = new StubPipelineLogger
    val stream = Stream.emits(
      List(
        ProcessingResult.Succeeded("117-SAMDT-1"),
        ProcessingResult.Failed("117-SAMDT-2", "boom"),
      )
    )
    val result = PipelineExecutor.execute[IO](stream, logger, pipelineName, runId).unsafeRunSync()
    result.code shouldBe 1
  }

  it should "return ExitCode.Success when all results are Skipped" in {
    val logger = new StubPipelineLogger
    val stream = Stream.emits(
      List(
        ProcessingResult.Skipped("117-SAMDT-1", "no-new-versions"),
        ProcessingResult.Skipped("117-SAMDT-2", "no-new-versions"),
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

  it should "log the summary with correct counts" in {
    val logger = new StubPipelineLogger
    val stream = Stream.emits(
      List(
        ProcessingResult.Succeeded("117-SAMDT-1"),
        ProcessingResult.Succeeded("117-SAMDT-2"),
        ProcessingResult.Failed("117-SAMDT-3", "api error"),
        ProcessingResult.Skipped("117-SAMDT-4", "no-new-versions"),
      )
    )
    val _          = PipelineExecutor.execute[IO](stream, logger, pipelineName, runId).unsafeRunSync()
    val summaryLog = logger.messages.find(_.contains("Pipeline completed"))
    val _          = summaryLog.exists(_.contains("4 processed")) shouldBe true
    val _          = summaryLog.exists(_.contains("3 succeeded")) shouldBe true
    summaryLog.exists(_.contains("1 failed")) shouldBe true
  }

  it should "propagate stream errors" in {
    val logger = new StubPipelineLogger
    val stream: Stream[IO, ProcessingResult] =
      Stream.emit(ProcessingResult.Succeeded("a")) ++ Stream.raiseError[IO](new RuntimeException("stream failure"))
    val result = PipelineExecutor.execute[IO](stream, logger, pipelineName, runId).attempt.unsafeRunSync()
    result.isLeft shouldBe true
  }

}
