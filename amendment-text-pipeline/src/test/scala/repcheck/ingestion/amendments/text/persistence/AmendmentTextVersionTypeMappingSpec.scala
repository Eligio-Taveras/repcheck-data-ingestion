package repcheck.ingestion.amendments.text.persistence

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import repcheck.ingestion.amendments.text.errors.UnsupportedAmendmentTextVersionCode

class AmendmentTextVersionTypeMappingSpec extends AnyFlatSpec with Matchers {

  "wireToStored" should "map SUB to Submitted" in {
    AmendmentTextVersionTypeMapping.wireToStored("SUB") shouldBe Right("Submitted")
  }

  it should "map MOD to Modified" in {
    AmendmentTextVersionTypeMapping.wireToStored("MOD") shouldBe Right("Modified")
  }

  it should "raise UnsupportedAmendmentTextVersionCode for unknown codes" in {
    val result = AmendmentTextVersionTypeMapping.wireToStored("NOPE")
    val _      = result.isLeft shouldBe true
    result.left.toOption shouldBe Some(UnsupportedAmendmentTextVersionCode("NOPE"))
  }

  it should "be case-sensitive (lowercase 'sub' is not 'SUB')" in {
    val result = AmendmentTextVersionTypeMapping.wireToStored("sub")
    result.isLeft shouldBe true
  }

  it should "raise on the empty string" in {
    val result = AmendmentTextVersionTypeMapping.wireToStored("")
    result.isLeft shouldBe true
  }

  "storedToWire" should "map Submitted to SUB" in {
    AmendmentTextVersionTypeMapping.storedToWire("Submitted") shouldBe Some("SUB")
  }

  it should "map Modified to MOD" in {
    AmendmentTextVersionTypeMapping.storedToWire("Modified") shouldBe Some("MOD")
  }

  it should "return None for unknown stored values" in {
    AmendmentTextVersionTypeMapping.storedToWire("Engrossed") shouldBe None
  }

}
