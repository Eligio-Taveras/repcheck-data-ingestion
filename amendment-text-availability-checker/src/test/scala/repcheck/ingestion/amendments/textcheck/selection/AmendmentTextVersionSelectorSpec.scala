package repcheck.ingestion.amendments.textcheck.selection

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import repcheck.shared.models.congress.dto.amendment.{AmendmentFormatDTO, AmendmentTextItemDTO}

class AmendmentTextVersionSelectorSpec extends AnyFlatSpec with Matchers {

  private def submittedHtml(url: String = "https://www.congress.gov/sub.htm"): AmendmentTextItemDTO =
    AmendmentTextItemDTO(
      `type` = Some("Submitted"),
      date = Some("2024-04-01T12:00:00Z"),
      formats = List(AmendmentFormatDTO(`type` = "HTML", url = url)),
    )

  private def modifiedPdf(url: String = "https://www.congress.gov/mod.pdf"): AmendmentTextItemDTO =
    AmendmentTextItemDTO(
      `type` = Some("Modified"),
      date = Some("2024-04-15T12:00:00Z"),
      formats = List(AmendmentFormatDTO(`type` = "PDF", url = url)),
    )

  private def submittedHtmlAndPdf(): AmendmentTextItemDTO =
    AmendmentTextItemDTO(
      `type` = Some("Submitted"),
      date = Some("2024-04-01T12:00:00Z"),
      formats = List(
        AmendmentFormatDTO("PDF", "https://www.congress.gov/sub.pdf"),
        AmendmentFormatDTO("HTML", "https://www.congress.gov/sub.htm"),
      ),
    )

  "versionTypeCode" should "map 'Submitted' to SUB" in {
    AmendmentTextVersionSelector.versionTypeCode(Some("Submitted")) shouldBe Some("SUB")
  }

  it should "map 'Modified' to MOD" in {
    AmendmentTextVersionSelector.versionTypeCode(Some("Modified")) shouldBe Some("MOD")
  }

  it should "return None for unknown types" in {
    AmendmentTextVersionSelector.versionTypeCode(Some("Bogus")) shouldBe None
  }

  it should "return None when no type is provided" in {
    AmendmentTextVersionSelector.versionTypeCode(None) shouldBe None
  }

  "selectAllNewVersions" should "prefer HTML over PDF when both formats are present in one version" in {
    val result = AmendmentTextVersionSelector.selectAllNewVersions(
      upstream = List(submittedHtmlAndPdf()),
      existing = Nil,
    )
    val _ = result.size shouldBe 1
    result.headOption.map(_._2.`type`) shouldBe Some("HTML")
  }

  it should "fall back to PDF when HTML is absent for that version" in {
    val pdfOnly = AmendmentTextItemDTO(
      `type` = Some("Submitted"),
      date = None,
      formats = List(AmendmentFormatDTO("PDF", "https://www.congress.gov/sub.pdf")),
    )
    val result = AmendmentTextVersionSelector.selectAllNewVersions(
      upstream = List(pdfOnly),
      existing = Nil,
    )
    result.headOption.map(_._2.`type`) shouldBe Some("PDF")
  }

  it should "skip versions with only unsupported formats (XML)" in {
    val xmlOnly = AmendmentTextItemDTO(
      `type` = Some("Submitted"),
      date = None,
      formats = List(AmendmentFormatDTO("XML", "https://www.congress.gov/sub.xml")),
    )
    val result = AmendmentTextVersionSelector.selectAllNewVersions(
      upstream = List(xmlOnly),
      existing = Nil,
    )
    result shouldBe empty
  }

  it should "skip versions whose 'type' is None or unrecognized" in {
    val unknown = AmendmentTextItemDTO(
      `type` = Some("Withdrawn"),
      date = None,
      formats = List(AmendmentFormatDTO("HTML", "https://example.com/foo.htm")),
    )
    val typeless = AmendmentTextItemDTO(
      `type` = None,
      date = None,
      formats = List(AmendmentFormatDTO("HTML", "https://example.com/bar.htm")),
    )
    val result = AmendmentTextVersionSelector.selectAllNewVersions(
      upstream = List(unknown, typeless),
      existing = Nil,
    )
    result shouldBe empty
  }

  it should "return both Submitted and Modified when both are new" in {
    val result = AmendmentTextVersionSelector.selectAllNewVersions(
      upstream = List(submittedHtml(), modifiedPdf()),
      existing = Nil,
    )
    val _     = result.size shouldBe 2
    val codes = result.flatMap { case (item, _) => AmendmentTextVersionSelector.versionTypeCode(item.`type`) }
    val _     = codes should contain("SUB")
    codes should contain("MOD")
  }

  it should "filter out already-ingested (versionTypeCode, formatType) tuples" in {
    val result = AmendmentTextVersionSelector.selectAllNewVersions(
      upstream = List(submittedHtml(), modifiedPdf()),
      existing = List(("SUB", "HTML")),
    )
    val _     = result.size shouldBe 1
    val codes = result.flatMap { case (item, _) => AmendmentTextVersionSelector.versionTypeCode(item.`type`) }
    codes shouldBe List("MOD")
  }

  it should "return nothing when every upstream tuple is already ingested" in {
    val result = AmendmentTextVersionSelector.selectAllNewVersions(
      upstream = List(submittedHtml(), modifiedPdf()),
      existing = List(("SUB", "HTML"), ("MOD", "PDF")),
    )
    result shouldBe empty
  }

  it should "return nothing when upstream is empty" in {
    AmendmentTextVersionSelector.selectAllNewVersions(Nil, Nil) shouldBe empty
  }

  it should "return all upstream tuples when existing is empty" in {
    val result = AmendmentTextVersionSelector.selectAllNewVersions(
      upstream = List(submittedHtml(), modifiedPdf()),
      existing = Nil,
    )
    result.size shouldBe 2
  }

  it should "treat (SUB, HTML) and (SUB, PDF) as distinct tuples (different formatType)" in {
    // If existing has (SUB, HTML) and upstream gives a Submitted that the selector resolves to PDF
    // (because no HTML present), the (SUB, PDF) tuple is still new.
    val pdfOnlySub = AmendmentTextItemDTO(
      `type` = Some("Submitted"),
      date = None,
      formats = List(AmendmentFormatDTO("PDF", "https://example.com/sub.pdf")),
    )
    val result = AmendmentTextVersionSelector.selectAllNewVersions(
      upstream = List(pdfOnlySub),
      existing = List(("SUB", "HTML")),
    )
    val _ = result.size shouldBe 1
    result.headOption.map(_._2.`type`) shouldBe Some("PDF")
  }

  it should "skip versions whose formats list is empty" in {
    val noFormats = AmendmentTextItemDTO(
      `type` = Some("Submitted"),
      date = None,
      formats = Nil,
    )
    AmendmentTextVersionSelector.selectAllNewVersions(List(noFormats), Nil) shouldBe empty
  }

  it should "keep HTML when it appears BEFORE PDF in the formats list (retain-best branch)" in {
    // Exercises the foldLeft branch where the running-best stays best (HTML=0 already kept,
    // subsequent PDF=1 rejected because 1 is NOT < 0). The reverse order ("HTML after PDF") is
    // covered by the earlier "prefer HTML over PDF" test, which exercises the swap branch.
    val htmlFirst = AmendmentTextItemDTO(
      `type` = Some("Submitted"),
      date = None,
      formats = List(
        AmendmentFormatDTO("HTML", "https://example.com/sub.htm"),
        AmendmentFormatDTO("PDF", "https://example.com/sub.pdf"),
      ),
    )
    val result = AmendmentTextVersionSelector.selectAllNewVersions(List(htmlFirst), Nil)
    result.headOption.map(_._2.`type`) shouldBe Some("HTML")
  }

}
