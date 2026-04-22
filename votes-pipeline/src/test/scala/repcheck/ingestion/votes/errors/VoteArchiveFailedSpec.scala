package repcheck.ingestion.votes.errors

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class VoteArchiveFailedSpec extends AnyFlatSpec with Matchers {

  "VoteArchiveFailed" should "include the vote id and detail in the message" in {
    val err = VoteArchiveFailed(voteId = 42L, detail = "history table full")
    val _   = err.getMessage should include("42")
    err.getMessage should include("history table full")
  }

  it should "preserve the cause when one is supplied" in {
    val cause = new RuntimeException("boom")
    val err   = VoteArchiveFailed(voteId = 1L, detail = "FK violation", cause = Some(cause))
    err.getCause shouldBe theSameInstanceAs(cause)
  }

  it should "leave no cause chained when None is supplied" in {
    val err           = VoteArchiveFailed(voteId = 1L, detail = "other")
    val hasCauseChain = Option(err.getCause).isDefined
    hasCauseChain shouldBe false
  }

  it should "be an Exception subtype so RetryWrapper can match it" in {
    VoteArchiveFailed(1L, "d") shouldBe a[Exception]
  }

}
