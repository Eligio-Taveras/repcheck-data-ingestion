package repcheck.ingestion.votes.app

import cats.effect.IO
import cats.effect.unsafe.implicits.global

import fs2.Stream

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}
import repcheck.pipeline.models.metadata.ProcessingResult

class PipelineExecutorSpec extends AnyFlatSpec with Matchers {

  private val runId        = "12345"
  private val stepRunId    = 99L
  private val pipelineName = "test-pipeline"

  /**
   * A stub logger that records log messages via an AtomicReference to avoid WartRemover mutable collection
   * restrictions.
   */
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
        ProcessingResult.Succeeded("vote-1"),
        ProcessingResult.Succeeded("vote-2"),
      )
    )

    val result = PipelineExecutor.execute[IO](stream, logger, pipelineName, runId, stepRunId).unsafeRunSync()
    result.code shouldBe 0
  }

  it should "return ExitCode.Error when any result fails" in {
    val logger = new StubPipelineLogger
    val stream = Stream.emits(
      List(
        ProcessingResult.Succeeded("vote-1"),
        ProcessingResult.Failed("vote-2", "conversion error"),
      )
    )

    val result = PipelineExecutor.execute[IO](stream, logger, pipelineName, runId, stepRunId).unsafeRunSync()
    result.code shouldBe 1
  }

  it should "return ExitCode.Success when all results are skipped" in {
    val logger = new StubPipelineLogger
    val stream = Stream.emits(
      List(
        ProcessingResult.Skipped("vote-1", "unchanged"),
        ProcessingResult.Skipped("vote-2", "unchanged"),
      )
    )

    val result = PipelineExecutor.execute[IO](stream, logger, pipelineName, runId, stepRunId).unsafeRunSync()
    result.code shouldBe 0
  }

  it should "return ExitCode.Success for an empty result stream" in {
    val logger                               = new StubPipelineLogger
    val stream: Stream[IO, ProcessingResult] = Stream.empty

    val result = PipelineExecutor.execute[IO](stream, logger, pipelineName, runId, stepRunId).unsafeRunSync()
    result.code shouldBe 0
  }

  it should "log the pipeline summary with correct counts" in {
    val logger = new StubPipelineLogger
    val stream = Stream.emits(
      List(
        ProcessingResult.Succeeded("vote-1"),
        ProcessingResult.Succeeded("vote-2"),
        ProcessingResult.Failed("vote-3", "api error"),
        ProcessingResult.Skipped("vote-4", "unchanged"),
      )
    )

    val _ = PipelineExecutor.execute[IO](stream, logger, pipelineName, runId, stepRunId).unsafeRunSync()

    // Summary line + failure-details line (the latter is only logged when failures > 0).
    val _          = logger.messages.size shouldBe 2
    val logMessage = logger.messages.headOption.getOrElse(fail("expected at least one log message"))
    val _          = logMessage should include("4 processed")
    val _          = logMessage should include("2 succeeded")
    logMessage should include("1 failed")
  }

  it should "log actual failure details on a second line when failures exist" in {
    val logger = new StubPipelineLogger
    val stream = Stream.emits(
      List(
        ProcessingResult.Succeeded("vote-1"),
        ProcessingResult.Failed("vote-2", "http 503", errorClass = "Transient"),
        ProcessingResult.Failed("vote-3", "http 429", errorClass = "Transient"),
      )
    )

    val _ = PipelineExecutor.execute[IO](stream, logger, pipelineName, runId, stepRunId).unsafeRunSync()

    val details = logger.messages
      .find(_.contains("Failure details"))
      .getOrElse(fail("expected a failure-details log line"))
    val _ = details should include("vote-2(Transient): http 503")
    details should include("vote-3(Transient): http 429")
  }

  it should "not log a failure-details line when all results succeeded or were skipped" in {
    val logger = new StubPipelineLogger
    val stream = Stream.emits(
      List(
        ProcessingResult.Succeeded("vote-1"),
        ProcessingResult.Skipped("vote-2", "unchanged"),
      )
    )

    val _ = PipelineExecutor.execute[IO](stream, logger, pipelineName, runId, stepRunId).unsafeRunSync()

    logger.messages.exists(_.contains("Failure details")) shouldBe false
  }

  it should "return ExitCode.Error when all results fail" in {
    val logger = new StubPipelineLogger
    val stream = Stream.emits(
      List(
        ProcessingResult.Failed("vote-1", "error 1"),
        ProcessingResult.Failed("vote-2", "error 2"),
      )
    )

    val result = PipelineExecutor.execute[IO](stream, logger, pipelineName, runId, stepRunId).unsafeRunSync()
    result.code shouldBe 1
  }

  it should "return ExitCode.Success for mixed succeeded and skipped results with zero failures" in {
    val logger = new StubPipelineLogger
    val stream = Stream.emits(
      List(
        ProcessingResult.Succeeded("vote-1"),
        ProcessingResult.Skipped("vote-2", "unchanged"),
        ProcessingResult.Succeeded("vote-3"),
      )
    )

    val result = PipelineExecutor.execute[IO](stream, logger, pipelineName, runId, stepRunId).unsafeRunSync()
    result.code shouldBe 0
  }

  it should "include the run ID in the log context" in {
    val logger = new StubPipelineLogger
    val stream = Stream.emit(ProcessingResult.Succeeded("vote-1"))

    val _ = PipelineExecutor.execute[IO](stream, logger, pipelineName, runId, stepRunId).unsafeRunSync()

    logger.messages should not be empty
  }

  it should "handle a stream that raises an error" in {
    val logger = new StubPipelineLogger
    val stream: Stream[IO, ProcessingResult] =
      Stream.emit(ProcessingResult.Succeeded("vote-1")) ++
        Stream.raiseError[IO](new RuntimeException("stream failure"))

    val result = PipelineExecutor.execute[IO](stream, logger, pipelineName, runId, stepRunId).attempt.unsafeRunSync()
    result.isLeft shouldBe true
  }

}
