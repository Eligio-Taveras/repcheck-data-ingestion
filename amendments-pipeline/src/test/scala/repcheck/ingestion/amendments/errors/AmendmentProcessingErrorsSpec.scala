package repcheck.ingestion.amendments.errors

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class AmendmentProcessingErrorsSpec extends AnyFlatSpec with Matchers {

  "AmendmentRecursionTooDeep" should "carry depth + naturalKey in its message" in {
    val err = AmendmentRecursionTooDeep(depth = 11, naturalKey = "117-SAMDT-100")
    val _   = err.getMessage should include("11")
    err.getMessage should include("117-SAMDT-100")
  }

  "AmendmentProcessingFailed" should "wrap the underlying cause and surface its message" in {
    val cause = new IllegalArgumentException("boom")
    val err   = AmendmentProcessingFailed("117-SAMDT-100", cause)
    val _     = err.getMessage should include("117-SAMDT-100")
    val _     = err.getMessage should include("boom")
    err.getCause shouldBe cause
  }

  "PoolSizingTooSmall" should "include the supplied detail in its message" in {
    val err = PoolSizingTooSmall("max-connections=5 < required 45")
    val _   = err.getMessage should include("max-connections=5")
    err.getMessage should include("45")
  }

}
