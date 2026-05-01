package repcheck.ingestion.bills.common

import java.time.{Instant, LocalDate}

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import repcheck.shared.models.congress.bill.TextVersionCode
import repcheck.shared.models.congress.common.{BillType, Chamber}
import repcheck.shared.models.congress.dos.bill.BillDO
import repcheck.shared.models.placeholder.HasPlaceholder

class BillPlaceholderSpec extends AnyFlatSpec with Matchers {

  // The shape this matches MUST stay consistent with HasPlaceholder[BillDO].placeholder() in
  // shared-models. The first test below pins that invariant — if a new detail field is added to
  // BillDO and only one side is updated, this test fails.
  "BillPlaceholder.isPlaceholder" should "agree with HasPlaceholder[BillDO].placeholder() — invariant lock" in {
    val factoryOutput = HasPlaceholder[BillDO].placeholder("119-HR-1")
    BillPlaceholder.isPlaceholder(factoryOutput) shouldBe true
  }

  it should "return true for a row with title='' and every detail Option = None (the in-DB shape)" in {
    val placeholder = BillDO(
      billId = 42L, // real DB id, not 0
      naturalKey = "118-HR-1",
      congress = 118, // real natural-key fields, overridden by the writer
      billType = BillType.HR,
      number = "1",
      title = "",
      originChamber = None,
      originChamberCode = None,
      introducedDate = None,
      policyArea = None,
      latestActionDate = None,
      latestActionText = None,
      constitutionalAuthorityText = None,
      sponsorMemberId = None,
      textUrl = None,
      textFormat = None,
      textVersionType = None,
      textDate = None,
      textContent = None,
      summaryText = None,
      summaryActionDesc = None,
      summaryActionDate = None,
      updateDate = Some(Instant.parse("2026-04-27T02:00:00Z")), // insertion-time, not None
      updateDateIncludingText = None,
      legislationUrl = None,
      apiUrl = None,
      createdAt = Some(Instant.parse("2026-04-27T02:00:00Z")),
      updatedAt = Some(Instant.parse("2026-04-27T02:00:00Z")),
      latestTextVersionId = None,
    )
    BillPlaceholder.isPlaceholder(placeholder) shouldBe true
  }

  it should "return false when title is non-empty" in {
    val notPlaceholder = HasPlaceholder[BillDO].placeholder("119-HR-1").copy(title = "Real Bill Title")
    BillPlaceholder.isPlaceholder(notPlaceholder) shouldBe false
  }

  it should "return false when originChamber is set" in {
    val notPlaceholder = HasPlaceholder[BillDO].placeholder("119-HR-1").copy(originChamber = Some(Chamber.House))
    BillPlaceholder.isPlaceholder(notPlaceholder) shouldBe false
  }

  it should "return false when introducedDate is set" in {
    val notPlaceholder =
      HasPlaceholder[BillDO].placeholder("119-HR-1").copy(introducedDate = Some(LocalDate.of(2025, 1, 3)))
    BillPlaceholder.isPlaceholder(notPlaceholder) shouldBe false
  }

  it should "return false when sponsorMemberId is set" in {
    val notPlaceholder = HasPlaceholder[BillDO].placeholder("119-HR-1").copy(sponsorMemberId = Some(42L))
    BillPlaceholder.isPlaceholder(notPlaceholder) shouldBe false
  }

  it should "return false when textVersionType is set" in {
    val notPlaceholder =
      HasPlaceholder[BillDO].placeholder("119-HR-1").copy(textVersionType = Some(TextVersionCode.IH))
    BillPlaceholder.isPlaceholder(notPlaceholder) shouldBe false
  }

}
