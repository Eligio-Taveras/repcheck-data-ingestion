package repcheck.ingestion.amendments.textcheck.errors

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class AmendmentTextCheckMissingAmendmentTypeSpec extends AnyFlatSpec with Matchers {

  "AmendmentTextCheckMissingAmendmentType" should "include the natural key and id in its message" in {
    val err = AmendmentTextCheckMissingAmendmentType("117-SAMDT-2137", 42L)
    val _   = err.getMessage should include("117-SAMDT-2137")
    val _   = err.getMessage should include("42")
    err.naturalKey shouldBe "117-SAMDT-2137"
  }

}
