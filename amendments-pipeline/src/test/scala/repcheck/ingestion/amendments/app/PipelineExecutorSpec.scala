package repcheck.ingestion.amendments.app

import java.util.concurrent.atomic.AtomicReference

import cats.effect.IO
import cats.effect.unsafe.implicits.global

import fs2.Stream

import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.when
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar
import repcheck.ingestion.amendments.observability.AmendmentMetrics
import repcheck.ingestion.amendments.pipeline.{AmendmentProcessor, PipelineRunSummary}
import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}
import repcheck.pipeline.models.metadata.ProcessingResult

class PipelineExecutorSpec extends AnyFlatSpec with Matchers with MockitoSugar {

  private val pipelineName = "amendments-pipeline"
  private val runId        = "run-1"

  private class CapturingLogger extends PipelineLogger[IO] {
    val messagesRef: AtomicReference[List[String]] = new AtomicReference(List.empty)
    val warningsRef: AtomicReference[List[String]] = new AtomicReference(List.empty)

    override def info(context: LogContext, message: String): IO[Unit] = IO {
      val _ = messagesRef.updateAndGet(_ :+ message)
    }

    override def warn(context: LogContext, message: String): IO[Unit] = IO {
      val _ = warningsRef.updateAndGet(_ :+ message)
    }

    override def error(context: LogContext, message: String, cause: Option[Throwable]): IO[Unit] = IO.unit
    override def debug(context: LogContext, message: String): IO[Unit]                           = IO.unit
  }

  private def stubProcessor(summary: PipelineRunSummary): AmendmentProcessor[IO] = {
    val p = mock[AmendmentProcessor[IO]]
    when(p.summarize(any[List[ProcessingResult]])).thenReturn(summary)
    p
  }

  "execute" should "return ExitCode.Success when no failure occurred" in {
    val stream =
      Stream.emits[IO, ProcessingResult](
        List(
          ProcessingResult.Succeeded("a-1"),
          ProcessingResult.Skipped("a-2", "unchanged"),
        )
      )
    val logger = new CapturingLogger
    val processor = stubProcessor(
      PipelineRunSummary(totalProcessed = 2, succeeded = 1, skipped = 1, failed = 0, eventsEmitted = 0, errors = Nil)
    )
    val code = PipelineExecutor
      .execute[IO](stream, processor, logger, pipelineName, runId, AmendmentMetrics.make())
      .unsafeRunSync()
    val _ = code.code shouldBe 0
    logger.messagesRef.get().exists(_.contains("Pipeline completed")) shouldBe true
  }

  it should "return ExitCode.Error and emit a warn log when a failure occurred" in {
    val stream =
      Stream.emits[IO, ProcessingResult](
        List(
          ProcessingResult.Succeeded("a-1"),
          ProcessingResult.Failed("a-2", "boom"),
        )
      )
    val logger = new CapturingLogger
    val processor = stubProcessor(
      PipelineRunSummary(
        totalProcessed = 2,
        succeeded = 1,
        skipped = 0,
        failed = 1,
        eventsEmitted = 0,
        errors = List("boom"),
      )
    )
    val code = PipelineExecutor
      .execute[IO](stream, processor, logger, pipelineName, runId, AmendmentMetrics.make())
      .unsafeRunSync()
    val _ = code.code shouldBe 1
    logger.warningsRef.get().exists(_.contains("boom")) shouldBe true
  }

  it should "log the metrics snapshot at the tail" in {
    val metrics = AmendmentMetrics.make()
    metrics.incrementDetailFetches()
    metrics.incrementOrphanResolved()
    val logger = new CapturingLogger
    val processor = stubProcessor(
      PipelineRunSummary(totalProcessed = 0, succeeded = 0, skipped = 0, failed = 0, eventsEmitted = 0, errors = Nil)
    )
    val _ = PipelineExecutor
      .execute[IO](Stream.empty, processor, logger, pipelineName, runId, metrics)
      .unsafeRunSync()

    logger.messagesRef.get().exists(m => m.contains("detailFetches=1") && m.contains("orphanResolved=1")) shouldBe true
  }

  it should "truncate the failure list after 20 entries" in {
    val many =
      (1 to 25).map(i => ProcessingResult.Failed(s"a-${i.toString}", s"reason-${i.toString}")).toList
    val logger = new CapturingLogger
    val processor = stubProcessor(
      PipelineRunSummary(
        totalProcessed = 25,
        succeeded = 0,
        skipped = 0,
        failed = 25,
        eventsEmitted = 0,
        errors = (1 to 25).map(i => s"reason-${i.toString}").toList,
      )
    )
    val _ = PipelineExecutor
      .execute[IO](
        Stream.emits[IO, ProcessingResult](many),
        processor,
        logger,
        pipelineName,
        runId,
        AmendmentMetrics.make(),
      )
      .unsafeRunSync()

    val warn = logger.warningsRef.get().headOption.getOrElse("")
    warn should include("(+5 more)")
  }

}
