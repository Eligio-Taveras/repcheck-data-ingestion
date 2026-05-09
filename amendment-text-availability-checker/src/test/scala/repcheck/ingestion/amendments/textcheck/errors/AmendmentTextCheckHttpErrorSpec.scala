package repcheck.ingestion.amendments.textcheck.errors

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import repcheck.ingestion.common.errors.HttpStatusError

class AmendmentTextCheckHttpErrorSpec extends AnyFlatSpec with Matchers {

  "AmendmentTextCheckHttpError" should "expose the status code via HttpStatusError" in {
    val err = AmendmentTextCheckHttpError(503, "service unavailable", attempt = 2)
    val _   = err shouldBe a[HttpStatusError]
    val _   = err.statusCode shouldBe 503
    err.body shouldBe "service unavailable"
  }

  it should "include status, attempt, and body in its message" in {
    val err = AmendmentTextCheckHttpError(429, "too many requests", attempt = 3)
    val _   = err.getMessage should include("429")
    val _   = err.getMessage should include("3")
    err.getMessage should include("too many requests")
  }

  it should "carry the attempt counter" in {
    val err = AmendmentTextCheckHttpError(500, "boom", attempt = 1)
    err.attempt shouldBe 1
  }

}
