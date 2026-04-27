package repcheck.ingestion.bills.summary.app

import cats.effect.IO
import cats.effect.unsafe.implicits.global

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import repcheck.ingestion.bills.summary.errors.StepRunIdInvalid

class BillSummaryPipelineSpec extends AnyFlatSpec with Matchers {

  "extractStepRunId" should "return the parsed Long when args(2) is a valid number" in {
    val result = BillSummaryPipeline.extractStepRunId[IO](List("cfg", "run-1", "42")).unsafeRunSync()
    result shouldBe 42L
  }

  it should "accept '0' for docker-compose / Ofelia placeholder use" in {
    val result = BillSummaryPipeline.extractStepRunId[IO](List("cfg", "run-1", "0")).unsafeRunSync()
    result shouldBe 0L
  }

  it should "raise StepRunIdInvalid when args(2) is missing" in {
    val outcome = BillSummaryPipeline.extractStepRunId[IO](List("cfg", "run-1")).attempt.unsafeRunSync()
    outcome match {
      case Left(StepRunIdInvalid("<missing>")) => succeed
      case Left(other)                         => fail(s"unexpected error: $other")
      case Right(value)                        => fail(s"expected failure, got success: $value")
    }
  }

  it should "raise StepRunIdInvalid when args(2) is blank" in {
    val outcome = BillSummaryPipeline.extractStepRunId[IO](List("cfg", "run-1", "   ")).attempt.unsafeRunSync()
    outcome match {
      case Left(StepRunIdInvalid("   ")) => succeed
      case Left(other)                   => fail(s"unexpected error: $other")
      case Right(value)                  => fail(s"expected failure, got success: $value")
    }
  }

  it should "raise StepRunIdInvalid when args(2) is not numeric" in {
    val outcome = BillSummaryPipeline.extractStepRunId[IO](List("cfg", "run-1", "abc")).attempt.unsafeRunSync()
    outcome match {
      case Left(StepRunIdInvalid("abc")) => succeed
      case Left(other)                   => fail(s"unexpected error: $other")
      case Right(value)                  => fail(s"expected failure, got success: $value")
    }
  }

  // (rateLimitedClient mechanics are now covered by ingestion-common's RateLimitedHttpClientSpec.)

}
