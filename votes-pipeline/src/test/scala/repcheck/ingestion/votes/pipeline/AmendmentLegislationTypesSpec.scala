package repcheck.ingestion.votes.pipeline

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import repcheck.shared.models.congress.amendment.AmendmentType

class AmendmentLegislationTypesSpec extends AnyFlatSpec with Matchers {

  "isAmendmentType" should "return true for HAMDT, SAMDT, and SUAMDT (case-insensitive)" in {
    val _ = AmendmentLegislationTypes.isAmendmentType("HAMDT") shouldBe true
    val _ = AmendmentLegislationTypes.isAmendmentType("SAMDT") shouldBe true
    val _ = AmendmentLegislationTypes.isAmendmentType("SUAMDT") shouldBe true
    val _ = AmendmentLegislationTypes.isAmendmentType("hamdt") shouldBe true
    AmendmentLegislationTypes.isAmendmentType("Samdt") shouldBe true
  }

  it should "return false for bill types and unknown values" in {
    val _ = AmendmentLegislationTypes.isAmendmentType("HR") shouldBe false
    val _ = AmendmentLegislationTypes.isAmendmentType("S") shouldBe false
    val _ = AmendmentLegislationTypes.isAmendmentType("") shouldBe false
    AmendmentLegislationTypes.isAmendmentType("XAMDT") shouldBe false
  }

  "parseAmendmentType" should "return Some for amendment-typed strings" in {
    val _ = AmendmentLegislationTypes.parseAmendmentType(Some("HAMDT")) shouldBe Some(AmendmentType.HAMDT)
    val _ = AmendmentLegislationTypes.parseAmendmentType(Some("SAMDT")) shouldBe Some(AmendmentType.SAMDT)
    AmendmentLegislationTypes.parseAmendmentType(Some("SUAMDT")) shouldBe Some(AmendmentType.SUAMDT)
  }

  it should "return None for None input" in {
    AmendmentLegislationTypes.parseAmendmentType(None) shouldBe None
  }

  it should "return None for bill-typed and unknown strings" in {
    val _ = AmendmentLegislationTypes.parseAmendmentType(Some("HR")) shouldBe None
    val _ = AmendmentLegislationTypes.parseAmendmentType(Some("S")) shouldBe None
    AmendmentLegislationTypes.parseAmendmentType(Some("XAMDT")) shouldBe None
  }

  "buildAmendmentNaturalKey" should "compose the canonical {congress}-{TYPE}-{number} shape" in {
    val _ =
      AmendmentLegislationTypes.buildAmendmentNaturalKey(117, AmendmentType.SAMDT, "2137") shouldBe "117-SAMDT-2137"
    val _ = AmendmentLegislationTypes.buildAmendmentNaturalKey(118, AmendmentType.HAMDT, "42") shouldBe "118-HAMDT-42"
    AmendmentLegislationTypes.buildAmendmentNaturalKey(98, AmendmentType.SUAMDT, "1") shouldBe "98-SUAMDT-1"
  }

}
