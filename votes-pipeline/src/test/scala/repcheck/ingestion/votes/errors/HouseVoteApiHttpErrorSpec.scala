package repcheck.ingestion.votes.errors

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import repcheck.ingestion.common.errors.HttpStatusError

class HouseVoteApiHttpErrorSpec extends AnyFlatSpec with Matchers {

  "HouseVoteApiHttpError" should "be an Exception subtype for cats-effect raiseError compatibility" in {
    HouseVoteApiHttpError(500, "boom") shouldBe a[Exception]
  }

  it should "implement HttpStatusError so the shared classifier can inspect statusCode" in {
    val err = HouseVoteApiHttpError(429, "rate limited")
    val _   = err shouldBe a[HttpStatusError]
    err.statusCode shouldBe 429
  }

  it should "carry the response body verbatim in the message for log debugging" in {
    val err = HouseVoteApiHttpError(502, "upstream exploded")
    val _   = err.getMessage should include("502")
    val _   = err.getMessage should include("upstream exploded")
    err.getMessage should include("house-vote")
  }

  it should "expose statusCode and body as case-class fields" in {
    val err = HouseVoteApiHttpError(404, "Not Found")
    val _   = err.statusCode shouldBe 404
    err.body shouldBe "Not Found"
  }

}
