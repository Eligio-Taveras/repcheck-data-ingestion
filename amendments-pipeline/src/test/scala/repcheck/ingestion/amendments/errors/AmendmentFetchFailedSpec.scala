package repcheck.ingestion.amendments.errors

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class AmendmentFetchFailedSpec extends AnyFlatSpec with Matchers {

  "AmendmentFetchFailed" should "include the natural key in its message" in {
    val cause = new RuntimeException("boom")
    val ex    = AmendmentFetchFailed("117-SAMDT-2137", cause)
    ex.getMessage should include("117-SAMDT-2137")
  }

  it should "include the cause's message" in {
    val cause = new RuntimeException("connection reset")
    val ex    = AmendmentFetchFailed("117-SAMDT-2137", cause)
    ex.getMessage should include("connection reset")
  }

  it should "preserve the cause for downstream classification" in {
    val cause = new java.io.IOException("network dropped")
    val ex    = AmendmentFetchFailed("117-SAMDT-2137", cause)
    ex.getCause shouldBe cause
  }

  it should "preserve the natural key as a field" in {
    val ex = AmendmentFetchFailed("117-HAMDT-42", new RuntimeException("x"))
    ex.naturalKey shouldBe "117-HAMDT-42"
  }

}
