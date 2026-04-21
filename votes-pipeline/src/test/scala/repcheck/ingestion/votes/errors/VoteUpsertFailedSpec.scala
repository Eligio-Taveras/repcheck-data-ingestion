package repcheck.ingestion.votes.errors

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class VoteUpsertFailedSpec extends AnyFlatSpec with Matchers {

  "VoteUpsertFailed" should "include the natural key and detail in the message" in {
    val err = VoteUpsertFailed(naturalKey = "house:118:1:17", detail = "duplicate row")
    val _   = err.getMessage should include("house:118:1:17")
    err.getMessage should include("duplicate row")
  }

  it should "preserve the cause when one is supplied" in {
    val cause = new RuntimeException("boom")
    val err   = VoteUpsertFailed(naturalKey = "senate:118:2:1", detail = "FK violation", cause = Some(cause))
    err.getCause shouldBe theSameInstanceAs(cause)
  }

  it should "leave no cause chained when None is supplied" in {
    val err           = VoteUpsertFailed(naturalKey = "senate:118:2:1", detail = "other")
    val hasCauseChain = Option(err.getCause).isDefined
    hasCauseChain shouldBe false
  }

  it should "be an Exception subtype so RetryWrapper can match it" in {
    VoteUpsertFailed("k", "d") shouldBe a[Exception]
  }

}
