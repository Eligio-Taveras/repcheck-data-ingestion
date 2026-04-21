package repcheck.ingestion.votes.errors

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class HouseVoteFetchFailedSpec extends AnyFlatSpec with Matchers {

  "HouseVoteFetchFailed.renderMessage" should "include congress and session when voteNumber is absent" in {
    val msg = HouseVoteFetchFailed.renderMessage(congress = 119, session = 1, voteNumber = None, detail = "boom")
    val _   = msg should include("congress=119")
    val _   = msg should include("session=1")
    val _   = msg should include("boom")
    msg should not include "vote="
  }

  it should "include vote=N when voteNumber is Some(N)" in {
    val msg = HouseVoteFetchFailed.renderMessage(congress = 119, session = 1, voteNumber = Some(17), detail = "bang")
    msg should include("vote=17")
  }

  "HouseVoteFetchFailed" should "propagate its cause through getCause" in {
    val cause = new RuntimeException("root cause")
    val err   = HouseVoteFetchFailed(congress = 119, session = 2, voteNumber = Some(42), detail = "fail", cause = cause)
    err.getCause shouldBe cause
  }

  it should "be an Exception subclass for cats-effect raiseError compatibility" in {
    HouseVoteFetchFailed(119, 1, None, "x", new Exception("y")) shouldBe a[Exception]
  }

  it should "expose all identifiers via case class fields for log-structured access" in {
    val err = HouseVoteFetchFailed(119, 2, Some(7), "why", new Exception("z"))
    val _   = err.congress shouldBe 119
    val _   = err.session shouldBe 2
    val _   = err.voteNumber shouldBe Some(7)
    err.detail shouldBe "why"
  }

}
