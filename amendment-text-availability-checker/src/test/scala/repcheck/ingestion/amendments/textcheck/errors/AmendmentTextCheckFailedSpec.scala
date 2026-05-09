package repcheck.ingestion.amendments.textcheck.errors

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class AmendmentTextCheckFailedSpec extends AnyFlatSpec with Matchers {

  "AmendmentTextCheckFailed" should "wrap the original cause and surface the natural key in the message" in {
    val cause = new RuntimeException("network down")
    val err   = AmendmentTextCheckFailed("117-SAMDT-2137", "Failed after retries", cause)
    val _     = err.getMessage should include("117-SAMDT-2137")
    val _     = err.getMessage should include("Failed after retries")
    err.getCause shouldBe cause
  }

  it should "expose the natural key field directly" in {
    val err = AmendmentTextCheckFailed("118-HAMDT-99", "boom", new RuntimeException("x"))
    val _   = err.naturalKey shouldBe "118-HAMDT-99"
    err.detail shouldBe "boom"
  }

}
