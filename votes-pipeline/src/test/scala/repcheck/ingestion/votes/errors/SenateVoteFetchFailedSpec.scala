package repcheck.ingestion.votes.errors

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SenateVoteFetchFailedSpec extends AnyFlatSpec with Matchers {

  "SenateVoteFetchFailed" should "format the error message with the vote coordinates when voteNumber is present" in {
    val cause = new RuntimeException("boom")
    val err = SenateVoteFetchFailed(
      congress = 119,
      session = 1,
      voteNumber = Some(17),
      detail = "decoder failed",
      cause = cause,
    )
    val _ = err.getMessage should include("vote 119-1-17")
    err.getMessage should include("decoder failed")
  }

  it should "format the error message for the vote index (voteNumber = None)" in {
    val err = SenateVoteFetchFailed(
      congress = 119,
      session = 1,
      voteNumber = None,
      detail = "transport broke",
      cause = new RuntimeException("t"),
    )
    val _ = err.getMessage should include("vote index 119-1")
    err.getMessage should include("transport broke")
  }

  it should "delegate statusCode to an HttpStatusError cause" in {
    val httpErr = SenateVoteXmlHttpError(503, "busy")
    val err = SenateVoteFetchFailed(
      congress = 119,
      session = 1,
      voteNumber = Some(1),
      detail = "fetch failed",
      cause = httpErr,
    )
    err.statusCode shouldBe 503
  }

  it should "fall back to statusCode 0 when the cause is not an HttpStatusError" in {
    val err = SenateVoteFetchFailed(
      congress = 119,
      session = 1,
      voteNumber = Some(1),
      detail = "decode failure",
      cause = new RuntimeException("plain"),
    )
    err.statusCode shouldBe 0
  }

  it should "preserve the original cause via Throwable#getCause" in {
    val cause = new RuntimeException("specific cause")
    val err = SenateVoteFetchFailed(
      congress = 119,
      session = 1,
      voteNumber = Some(1),
      detail = "fetch failed",
      cause = cause,
    )
    err.getCause shouldBe theSameInstanceAs(cause)
  }

}
