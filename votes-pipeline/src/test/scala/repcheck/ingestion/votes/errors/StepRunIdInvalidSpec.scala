package repcheck.ingestion.votes.errors

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class StepRunIdInvalidSpec extends AnyFlatSpec with Matchers {

  "StepRunIdInvalid" should "render the raw value in the message for debugging" in {
    val ex = StepRunIdInvalid("not-a-number")
    val _  = ex.getMessage should include("not-a-number")
    ex.getMessage should include("stepRunId")
  }

  it should "expose the raw value through the case class field" in {
    StepRunIdInvalid("abc").rawValue shouldBe "abc"
  }

  it should "distinguish the missing-arg case by message content" in {
    StepRunIdInvalid("<missing>").getMessage should include("<missing>")
  }

  it should "be an Exception" in {
    StepRunIdInvalid("x") shouldBe a[Exception]
  }

}
