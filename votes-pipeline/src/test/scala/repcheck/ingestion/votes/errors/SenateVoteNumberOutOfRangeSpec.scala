package repcheck.ingestion.votes.errors

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SenateVoteNumberOutOfRangeSpec extends AnyFlatSpec with Matchers {

  "SenateVoteNumberOutOfRange" should "include the vote number and reason in its message" in {
    val err = SenateVoteNumberOutOfRange(100000, "5 digits max")
    val _   = err.getMessage should include("100000")
    err.getMessage should include("5 digits max")
  }

  it should "expose the vote number and reason via case-class accessors" in {
    val err = SenateVoteNumberOutOfRange(-3, "negative")
    val _   = err.voteNumber shouldBe -3
    err.reason shouldBe "negative"
  }

}
