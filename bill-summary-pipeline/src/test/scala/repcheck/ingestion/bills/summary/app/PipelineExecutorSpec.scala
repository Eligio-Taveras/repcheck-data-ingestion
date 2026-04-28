package repcheck.ingestion.bills.summary.app

import cats.effect.IO
import cats.effect.unsafe.implicits.global

import fs2.Stream

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}
import repcheck.pipeline.models.metadata.ProcessingResult

class PipelineExecutorSpec extends AnyFlatSpec with Matchers {

  private val runId        = "test-bill-summary-run"
  private val stepRunId    = 99L
  private val pipelineName = "bill-summary-pipeline"

  /**
   * A stub logger that records log messages via an AtomicReference to avoid WartRemover mutable-collection
   * restrictions. Mirrors the votes-pipeline `PipelineExecutorSpec` stub — kept local because it's the only consumer
   * for now and the typeclass is one method per level.
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
        ProcessingResult.Succeeded("119-HR-1"),
        ProcessingResult.Succeeded("119-HR-2"),
      )
    )

    val result = PipelineExecutor.execute[IO](stream, logger, pipelineName, runId, stepRunId).unsafeRunSync()
    result.code shouldBe 0
  }

  it should "return ExitCode.Error when any result fails" in {
    val logger = new StubPipelineLogger
    val stream = Stream.emits(
      List(
        ProcessingResult.Succeeded("119-HR-1"),
        ProcessingResult.Failed("119-HR-2", "conversion error"),
      )
    )

    val result = PipelineExecutor.execute[IO](stream, logger, pipelineName, runId, stepRunId).unsafeRunSync()
    result.code shouldBe 1
  }

  it should "return ExitCode.Success when all results are skipped" in {
    val logger = new StubPipelineLogger
    val stream = Stream.emits(
      List(
        ProcessingResult.Skipped("119-HR-1", "already-at-or-past-stage:RH"),
        ProcessingResult.Skipped("119-HR-2", "already-at-or-past-stage:EAS"),
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

  it should "log the pipeline summary with correct counts (Monoid fold across mixed results)" in {
    val logger = new StubPipelineLogger
    val stream = Stream.emits(
      List(
        ProcessingResult.Succeeded("119-HR-1"),
        ProcessingResult.Succeeded("119-HR-2"),
        ProcessingResult.Failed("119-HR-3", "api error"),
        ProcessingResult.Skipped("119-HR-4", "unchanged"),
      )
    )

    val _ = PipelineExecutor.execute[IO](stream, logger, pipelineName, runId, stepRunId).unsafeRunSync()

    val _          = logger.messages.size shouldBe 2
    val logMessage = logger.messages.headOption.getOrElse(fail("expected at least one log message"))
    val _          = logMessage should include("4 processed")
    // Skipped results count toward succeeded (per pipeline-models v0.1.21 — idempotent skip is a successful no-op).
    val _ = logMessage should include("3 succeeded")
    logMessage should include("1 failed")
  }

  it should "log actual failure details on a second line when failures exist" in {
    val logger = new StubPipelineLogger
    val stream = Stream.emits(
      List(
        ProcessingResult.Succeeded("119-HR-1"),
        ProcessingResult.Failed("119-HR-2", "http 503", errorClass = "Transient"),
        ProcessingResult.Failed("119-HR-3", "http 429", errorClass = "Transient"),
      )
    )

    val _ = PipelineExecutor.execute[IO](stream, logger, pipelineName, runId, stepRunId).unsafeRunSync()

    val details = logger.messages
      .find(_.contains("Failure details"))
      .getOrElse(fail("expected a failure-details log line"))
    val _ = details should include("119-HR-2(Transient): http 503")
    details should include("119-HR-3(Transient): http 429")
  }

  it should "not log a failure-details line when all results succeeded or were skipped" in {
    val logger = new StubPipelineLogger
    val stream = Stream.emits(
      List(
        ProcessingResult.Succeeded("119-HR-1"),
        ProcessingResult.Skipped("119-HR-2", "unchanged"),
      )
    )

    val _ = PipelineExecutor.execute[IO](stream, logger, pipelineName, runId, stepRunId).unsafeRunSync()

    logger.messages.exists(_.contains("Failure details")) shouldBe false
  }

  it should "return ExitCode.Error when all results fail" in {
    val logger = new StubPipelineLogger
    val stream = Stream.emits(
      List(
        ProcessingResult.Failed("119-HR-1", "error 1"),
        ProcessingResult.Failed("119-HR-2", "error 2"),
      )
    )

    val result = PipelineExecutor.execute[IO](stream, logger, pipelineName, runId, stepRunId).unsafeRunSync()
    result.code shouldBe 1
  }

  it should "return ExitCode.Success for mixed succeeded and skipped results with zero failures" in {
    val logger = new StubPipelineLogger
    val stream = Stream.emits(
      List(
        ProcessingResult.Succeeded("119-HR-1"),
        ProcessingResult.Skipped("119-HR-2", "unchanged"),
        ProcessingResult.Succeeded("119-HR-3"),
      )
    )

    val result = PipelineExecutor.execute[IO](stream, logger, pipelineName, runId, stepRunId).unsafeRunSync()
    result.code shouldBe 0
  }

  it should "stream-aggregate without buffering all results in memory" in {
    // Emit 10k results — if PipelineExecutor compiled to a List internally we'd allocate ~10k Succeeded objects in
    // memory at once. With the Monoid fold path, only the rolling StepProgress accumulator is kept. We assert on the
    // final exit code + total count, which proves the fold consumed every emitted value.
    val logger      = new StubPipelineLogger
    val totalEvents = 10000
    val stream =
      Stream.emits((1 to totalEvents).toList.map(i => ProcessingResult.Succeeded(s"119-HR-$i")))

    val result  = PipelineExecutor.execute[IO](stream, logger, pipelineName, runId, stepRunId).unsafeRunSync()
    val _       = result.code shouldBe 0
    val summary = logger.messages.headOption.getOrElse(fail("expected at least one summary log line"))
    summary should include(s"$totalEvents processed")
  }

  it should "handle a stream that raises an error" in {
    val logger = new StubPipelineLogger
    val stream: Stream[IO, ProcessingResult] =
      Stream.emit(ProcessingResult.Succeeded("119-HR-1")) ++
        Stream.raiseError[IO](new java.util.NoSuchElementException("stream failure"))

    val result = PipelineExecutor.execute[IO](stream, logger, pipelineName, runId, stepRunId).attempt.unsafeRunSync()
    result.isLeft shouldBe true
  }

}
