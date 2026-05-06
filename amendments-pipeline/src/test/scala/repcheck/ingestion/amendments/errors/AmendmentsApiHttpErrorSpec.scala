package repcheck.ingestion.amendments.errors

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import repcheck.ingestion.common.errors.HttpStatusError

class AmendmentsApiHttpErrorSpec extends AnyFlatSpec with Matchers {

  "AmendmentsApiHttpError" should "expose the status code via HttpStatusError" in {
    val err: HttpStatusError = AmendmentsApiHttpError(429, "rate limit", attempt = 1)
    err.statusCode shouldBe 429
  }

  it should "include status, attempt, and body in its message" in {
    val ex = AmendmentsApiHttpError(503, "service unavailable", attempt = 2)
    val _  = ex.getMessage should include("503")
    val _  = ex.getMessage should include("attempt 2")
    ex.getMessage should include("service unavailable")
  }

  it should "preserve all fields" in {
    val ex = AmendmentsApiHttpError(404, "not found", attempt = 1)
    val _  = ex.statusCode shouldBe 404
    val _  = ex.body shouldBe "not found"
    ex.attempt shouldBe 1
  }

}
