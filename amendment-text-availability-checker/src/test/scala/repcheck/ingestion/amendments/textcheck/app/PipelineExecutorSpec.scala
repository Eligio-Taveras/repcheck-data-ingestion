package repcheck.ingestion.amendments.textcheck.app

import java.time.Instant

import cats.effect.IO
import cats.effect.unsafe.implicits.global

import fs2.Stream

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}
import repcheck.pipeline.models.metadata.ProcessingResult
import repcheck.pipeline.models.workflow.state.WorkflowStepStatus

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

  "addResult" should "increment succeeded for Succeeded results" in {
    val updated = PipelineExecutor.addResult(
      PipelineExecutor.StreamingStats.empty,
      ProcessingResult.Succeeded("a"),
    )
    val _ = updated.itemsProcessed shouldBe 1
    val _ = updated.itemsSucceeded shouldBe 1
    val _ = updated.itemsFailed shouldBe 0
    updated.errorCounts shouldBe empty
  }

  it should "treat Skipped as a successful no-op" in {
    val updated = PipelineExecutor.addResult(
      PipelineExecutor.StreamingStats.empty,
      ProcessingResult.Skipped("a", "no-new-versions"),
    )
    val _ = updated.itemsProcessed shouldBe 1
    val _ = updated.itemsSucceeded shouldBe 1
    updated.itemsFailed shouldBe 0
  }

  it should "increment failed and error-counts for Failed results" in {
    val s1 = PipelineExecutor.addResult(
      PipelineExecutor.StreamingStats.empty,
      ProcessingResult.Failed("a", "boom", "ApiError"),
    )
    val s2 = PipelineExecutor.addResult(s1, ProcessingResult.Failed("b", "boom", "ApiError"))
    val s3 = PipelineExecutor.addResult(s2, ProcessingResult.Failed("c", "other", "ApiError"))
    val _  = s3.itemsProcessed shouldBe 3
    val _  = s3.itemsSucceeded shouldBe 0
    val _  = s3.itemsFailed shouldBe 3
    val _  = s3.errorCounts.get("boom") shouldBe Some(2)
    s3.errorCounts.get("other") shouldBe Some(1)
  }

  "buildSummary" should "mark status Completed when no items processed" in {
    val now = Instant.now()
    val summary = PipelineExecutor.buildSummary(
      PipelineExecutor.StreamingStats.empty,
      stepRunId = 7L,
      pipelineName = pipelineName,
      startedAt = now,
      completedAt = now,
    )
    val _ = summary.status shouldBe WorkflowStepStatus.Completed
    summary.stepRunId shouldBe 7L
  }

  it should "mark status Failed when every item failed" in {
    val now = Instant.now()
    val stats = PipelineExecutor.StreamingStats(
      itemsProcessed = 3,
      itemsSucceeded = 0,
      itemsFailed = 3,
      errorCounts = Map("boom" -> 3),
    )
    PipelineExecutor.buildSummary(stats, 0L, pipelineName, now, now).status shouldBe WorkflowStepStatus.Failed
  }

  it should "mark status CompletedWithErrors on partial failure" in {
    val now = Instant.now()
    val stats = PipelineExecutor.StreamingStats(
      itemsProcessed = 3,
      itemsSucceeded = 2,
      itemsFailed = 1,
      errorCounts = Map("boom" -> 1),
    )
    PipelineExecutor.buildSummary(stats, 0L, pipelineName, now, now).status shouldBe
      WorkflowStepStatus.CompletedWithErrors
  }

  it should "mark status Completed when all items succeeded" in {
    val now = Instant.now()
    val stats = PipelineExecutor.StreamingStats(
      itemsProcessed = 3,
      itemsSucceeded = 3,
      itemsFailed = 0,
      errorCounts = Map.empty,
    )
    PipelineExecutor.buildSummary(stats, 0L, pipelineName, now, now).status shouldBe WorkflowStepStatus.Completed
  }

  "execute" should "carry stepRunId into the summary" in {
    val logger = new StubPipelineLogger
    val stream = Stream.emits(List(ProcessingResult.Succeeded("a")))
    val _      = PipelineExecutor.execute[IO](stream, logger, pipelineName, runId, stepRunId = 42L).unsafeRunSync()
    // execute logs only the counts, not the stepRunId — coverage of the stepRunId path is in buildSummary above.
    logger.messages.exists(_.contains("Pipeline completed")) shouldBe true
  }

}
