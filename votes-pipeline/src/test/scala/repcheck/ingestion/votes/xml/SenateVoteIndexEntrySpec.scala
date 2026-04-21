package repcheck.ingestion.votes.xml

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SenateVoteIndexEntrySpec extends AnyFlatSpec with Matchers {

  "SenateVoteIndexEntry" should "expose all fields" in {
    val entry = SenateVoteIndexEntry(voteNumber = 17, voteDate = "18-Dec", question = "Q", result = "R")
    val _     = entry.voteNumber shouldBe 17
    val _     = entry.voteDate shouldBe "18-Dec"
    val _     = entry.question shouldBe "Q"
    entry.result shouldBe "R"
  }

  it should "use case-class equality" in {
    val a = SenateVoteIndexEntry(1, "d", "q", "r")
    val b = SenateVoteIndexEntry(1, "d", "q", "r")
    a shouldBe b
  }

}
