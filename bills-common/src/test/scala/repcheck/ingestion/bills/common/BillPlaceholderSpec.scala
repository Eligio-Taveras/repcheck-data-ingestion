package repcheck.ingestion.bills.common

import java.time.{Instant, LocalDate}

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import repcheck.shared.models.congress.common.BillType
import repcheck.shared.models.congress.dos.bill.BillDO
import repcheck.shared.models.placeholder.HasPlaceholder

class BillPlaceholderSpec extends AnyFlatSpec with Matchers {

  // The factory output starts every metadata-owned field as None, so a freshly minted placeholder must
  // be classified as a placeholder. This pins the contract between the writer-side factory and the
  // reader-side detector — adding a new metadata-owned field to BillDO should fail this test until both
  // sides are updated together.
  "BillPlaceholder.isPlaceholder" should "return true for HasPlaceholder[BillDO].placeholder() — invariant lock" in {
    val factoryOutput = HasPlaceholder[BillDO].placeholder("119-HR-1")
    BillPlaceholder.isPlaceholder(factoryOutput) shouldBe true
  }

  it should "return true when sponsorMemberId is None even if every other metadata field is populated" in {
    val partial = hydratedBase.copy(sponsorMemberId = None)
    BillPlaceholder.isPlaceholder(partial) shouldBe true
  }

  it should "return true when introducedDate is None even if every other metadata field is populated" in {
    val partial = hydratedBase.copy(introducedDate = None)
    BillPlaceholder.isPlaceholder(partial) shouldBe true
  }

  it should "return true when latestActionText is None even if every other metadata field is populated" in {
    val partial = hydratedBase.copy(latestActionText = None)
    BillPlaceholder.isPlaceholder(partial) shouldBe true
  }

  it should "return true when updateDate is None even if every other metadata field is populated" in {
    val partial = hydratedBase.copy(updateDate = None)
    BillPlaceholder.isPlaceholder(partial) shouldBe true
  }

  // The four metadata-owned columns (sponsor_member_id, introduced_date, latest_action_text, update_date)
  // are all non-null on a fully hydrated row, so isPlaceholder = false and the caller falls through to
  // the update_date comparison for the unchanged-skip path. text_* and summary_* are intentionally NOT
  // checked — they belong to bill-text-pipeline and bill-summary-pipeline respectively.
  it should "return false when all metadata-owned fields are populated, regardless of text/summary fields" in {
    BillPlaceholder.isPlaceholder(hydratedBase) shouldBe false
  }

  it should "return false when text/summary fields are still None on an otherwise hydrated row" in {
    val onlyMetadataHydrated = hydratedBase.copy(
      textVersionType = None
    )
    BillPlaceholder.isPlaceholder(onlyMetadataHydrated) shouldBe false
  }

  // A fully hydrated row from the metadata pipeline's perspective: every metadata-owned field
  // populated. Other fields (text_*, summary_*, etc.) can stay None — they belong to other pipelines.
  private val hydratedBase: BillDO =
    HasPlaceholder[BillDO]
      .placeholder("119-HR-1")
      .copy(
        billId = 42L,
        congress = 119,
        billType = BillType.HR,
        number = "1",
        title = "Real Bill",
        sponsorMemberId = Some(100L),
        introducedDate = Some(LocalDate.of(2025, 1, 3)),
        latestActionText = Some("Referred to committee"),
        updateDate = Some(Instant.parse("2025-02-01T00:00:00Z")),
      )

}
