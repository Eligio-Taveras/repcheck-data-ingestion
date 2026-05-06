package repcheck.ingestion.amendments.pipeline

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import repcheck.shared.models.congress.dto.amendment.{AmendedAmendmentDTO, AmendedBillDTO, AmendmentListItemDTO}

class AmendmentNaturalKeysSpec extends AnyFlatSpec with Matchers {

  "fromListItem" should "uppercase the type segment" in {
    val item = AmendmentListItemDTO(
      congress = 117,
      number = "100",
      amendmentType = Some("samdt"),
      description = None,
      latestAction = None,
      updateDate = None,
      url = None,
    )
    AmendmentNaturalKeys.fromListItem(item) shouldBe "117-SAMDT-100"
  }

  it should "fall back to UNKNOWN when amendmentType is missing" in {
    val item = AmendmentListItemDTO(
      congress = 117,
      number = "100",
      amendmentType = None,
      description = None,
      latestAction = None,
      updateDate = None,
      url = None,
    )
    AmendmentNaturalKeys.fromListItem(item) shouldBe "117-UNKNOWN-100"
  }

  "parentAmendmentNaturalKey" should "build the canonical natural key" in {
    val parent = AmendedAmendmentDTO(
      congress = Some(117),
      number = Some("100"),
      amendmentType = Some("samdt"),
      purpose = None,
      updateDate = None,
      url = None,
    )
    AmendmentNaturalKeys.parentAmendmentNaturalKey(parent) shouldBe Some("117-SAMDT-100")
  }

  it should "return None when any field is missing" in {
    val noCongress = AmendedAmendmentDTO(
      congress = None,
      number = Some("100"),
      amendmentType = Some("samdt"),
      purpose = None,
      updateDate = None,
      url = None,
    )
    val _ = AmendmentNaturalKeys.parentAmendmentNaturalKey(noCongress) shouldBe None

    val noType = AmendedAmendmentDTO(
      congress = Some(117),
      number = Some("100"),
      amendmentType = None,
      purpose = None,
      updateDate = None,
      url = None,
    )
    val _ = AmendmentNaturalKeys.parentAmendmentNaturalKey(noType) shouldBe None

    val noNumber = AmendedAmendmentDTO(
      congress = Some(117),
      number = None,
      amendmentType = Some("samdt"),
      purpose = None,
      updateDate = None,
      url = None,
    )
    AmendmentNaturalKeys.parentAmendmentNaturalKey(noNumber) shouldBe None
  }

  "amendedBillNaturalKey" should "build the canonical natural key" in {
    val bill = AmendedBillDTO(
      congress = Some(117),
      number = Some("3684"),
      originChamber = None,
      originChamberCode = None,
      title = None,
      billType = Some("hr"),
      url = None,
      updateDateIncludingText = None,
    )
    AmendmentNaturalKeys.amendedBillNaturalKey(bill) shouldBe Some("117-HR-3684")
  }

  it should "return None when any field is missing" in {
    val noCongress = AmendedBillDTO(
      congress = None,
      number = Some("3684"),
      originChamber = None,
      originChamberCode = None,
      title = None,
      billType = Some("hr"),
      url = None,
      updateDateIncludingText = None,
    )
    AmendmentNaturalKeys.amendedBillNaturalKey(noCongress) shouldBe None
  }

}
