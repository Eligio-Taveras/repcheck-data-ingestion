package repcheck.ingestion.bills.summary.app

import cats.effect.IO
import cats.effect.unsafe.implicits.global

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import repcheck.ingestion.common.errors.StepRunIdInvalid
import repcheck.ingestion.common.execution.PipelineBootstrap

/**
 * Step-run-id parsing now lives in the shared [[PipelineBootstrap.extractStepRunId]] (3-arg launcher contract); this
 * spec pins the contract the bill-summary launcher relies on — args(2) parses to a `Long`, and a missing / blank /
 * non-numeric value surfaces [[StepRunIdInvalid]].
 */
class BillSummaryPipelineSpec extends AnyFlatSpec with Matchers {

  "PipelineBootstrap.extractStepRunId" should "return the parsed Long when args(2) is a valid number" in {
    PipelineBootstrap.extractStepRunId[IO](List("{}", "1", "42")).unsafeRunSync().value shouldBe 42L
  }

  it should "accept '0' for docker-compose / Ofelia placeholder use" in {
    PipelineBootstrap.extractStepRunId[IO](List("{}", "1", "0")).unsafeRunSync().value shouldBe 0L
  }

  it should "raise StepRunIdInvalid when args(2) is missing" in {
    val outcome = PipelineBootstrap.extractStepRunId[IO](List("{}", "1")).attempt.unsafeRunSync()
    outcome match {
      case Left(e: StepRunIdInvalid) => e.getMessage should include("missing or invalid")
      case Left(other)               => fail(s"unexpected error: $other")
      case Right(value)              => fail(s"expected failure, got success: $value")
    }
  }

  it should "raise StepRunIdInvalid when args(2) is blank" in {
    val outcome = PipelineBootstrap.extractStepRunId[IO](List("{}", "1", "   ")).attempt.unsafeRunSync()
    outcome match {
      case Left(e: StepRunIdInvalid) => e.getMessage should include("missing or invalid")
      case Left(other)               => fail(s"unexpected error: $other")
      case Right(value)              => fail(s"expected failure, got success: $value")
    }
  }

  it should "raise StepRunIdInvalid when args(2) is not numeric" in {
    val outcome = PipelineBootstrap.extractStepRunId[IO](List("{}", "1", "abc")).attempt.unsafeRunSync()
    outcome match {
      case Left(e: StepRunIdInvalid) => e.getMessage should include("abc")
      case Left(other)               => fail(s"unexpected error: $other")
      case Right(value)              => fail(s"expected failure, got success: $value")
    }
  }

}
