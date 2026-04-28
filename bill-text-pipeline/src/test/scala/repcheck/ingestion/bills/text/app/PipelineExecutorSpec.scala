package repcheck.ingestion.bills.text.app

import cats.effect.IO
import cats.effect.unsafe.implicits.global

import fs2.Stream

import org.mockito.ArgumentMatchers.{any, eq => eqTo}
import org.mockito.Mockito.{verify, when}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar
import repcheck.ingestion.common.execution.WorkflowStateUpdater
import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}
import repcheck.pipeline.models.metadata.ProcessingResult

class PipelineExecutorSpec extends AnyFlatSpec with Matchers with MockitoSugar {

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
        ProcessingResult.Succeeded("bill-1"),
        ProcessingResult.Succeeded("bill-2"),
      )
    )

    val result = PipelineExecutor.execute[IO](stream, logger, pipelineName, runId).unsafeRunSync()
    result.code shouldBe 0
  }

  it should "return ExitCode.Error when any result fails" in {
    val logger = new StubPipelineLogger
    val stream = Stream.emits(
      List(
        ProcessingResult.Succeeded("bill-1"),
        ProcessingResult.Failed("bill-2", "conversion error"),
      )
    )

    val result = PipelineExecutor.execute[IO](stream, logger, pipelineName, runId).unsafeRunSync()
    result.code shouldBe 1
  }

  it should "return ExitCode.Success when all results are skipped" in {
    val logger = new StubPipelineLogger
    val stream = Stream.emits(
      List(
        ProcessingResult.Skipped("bill-1", "unchanged"),
        ProcessingResult.Skipped("bill-2", "unchanged"),
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
        ProcessingResult.Succeeded("bill-1"),
        ProcessingResult.Succeeded("bill-2"),
        ProcessingResult.Failed("bill-3", "api error"),
        ProcessingResult.Skipped("bill-4", "unchanged"),
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

  it should "handle a stream that raises an error" in {
    val logger = new StubPipelineLogger
    val stream: Stream[IO, ProcessingResult] =
      Stream.emit(ProcessingResult.Succeeded("bill-1")) ++
        Stream.raiseError[IO](new RuntimeException("stream failure"))

    val result = PipelineExecutor.execute[IO](stream, logger, pipelineName, runId).attempt.unsafeRunSync()
    result.isLeft shouldBe true
  }

  // --- WorkflowStateUpdater integration tests ---

  private def buildMockUpdater(): WorkflowStateUpdater[IO] = {
    val mockUpdater = mock[WorkflowStateUpdater[IO]]
    when(mockUpdater.recordStepStarted(any[String](), any[String]())).thenReturn(IO.unit)
    when(mockUpdater.recordStepCompleted(any[String](), any[String]())).thenReturn(IO.unit)
    when(mockUpdater.recordStepFailed(any[String](), any[String](), any[String]())).thenReturn(IO.unit)
    mockUpdater
  }

  it should "call recordStepStarted when WorkflowStateUpdater is provided" in {
    val logger      = new StubPipelineLogger
    val mockUpdater = buildMockUpdater()
    val stream      = Stream.emit(ProcessingResult.Succeeded("bill-1"))

    val _ = PipelineExecutor
      .execute[IO](
        stream,
        logger,
        pipelineName,
        12345L,
        workflowStateUpdater = Some(mockUpdater),
      )
      .unsafeRunSync()

    verify(mockUpdater).recordStepStarted(eqTo("12345"), eqTo("test-pipeline"))
  }

  it should "call recordStepCompleted when all results succeed" in {
    val logger      = new StubPipelineLogger
    val mockUpdater = buildMockUpdater()
    val stream = Stream.emits(
      List(
        ProcessingResult.Succeeded("bill-1"),
        ProcessingResult.Succeeded("bill-2"),
      )
    )

    val _ = PipelineExecutor
      .execute[IO](
        stream,
        logger,
        pipelineName,
        12345L,
        workflowStateUpdater = Some(mockUpdater),
      )
      .unsafeRunSync()

    verify(mockUpdater).recordStepCompleted(eqTo("12345"), eqTo("test-pipeline"))
  }

  it should "call recordStepFailed when any result fails" in {
    val logger      = new StubPipelineLogger
    val mockUpdater = buildMockUpdater()
    val stream = Stream.emits(
      List(
        ProcessingResult.Succeeded("bill-1"),
        ProcessingResult.Failed("bill-2", "conversion error"),
      )
    )

    val _ = PipelineExecutor
      .execute[IO](
        stream,
        logger,
        pipelineName,
        12345L,
        workflowStateUpdater = Some(mockUpdater),
      )
      .unsafeRunSync()

    verify(mockUpdater).recordStepFailed(
      eqTo("12345"),
      eqTo("test-pipeline"),
      eqTo("1 of 2 items failed"),
    )
  }

  it should "use the run ID for workflow state updates" in {
    val logger      = new StubPipelineLogger
    val mockUpdater = buildMockUpdater()
    val stream      = Stream.emit(ProcessingResult.Succeeded("bill-1"))

    val _ = PipelineExecutor
      .execute[IO](
        stream,
        logger,
        pipelineName,
        99999L,
        workflowStateUpdater = Some(mockUpdater),
      )
      .unsafeRunSync()

    val _ = verify(mockUpdater).recordStepStarted(eqTo("99999"), eqTo("test-pipeline"))
    val _ = verify(mockUpdater).recordStepCompleted(eqTo("99999"), eqTo("test-pipeline"))
  }

}
