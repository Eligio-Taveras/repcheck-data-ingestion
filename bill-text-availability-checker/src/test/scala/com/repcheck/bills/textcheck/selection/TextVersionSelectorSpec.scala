package com.repcheck.bills.textcheck.selection

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import repcheck.shared.models.congress.dto.bill.{FormatDTO, TextVersionDTO}

import com.repcheck.bills.textcheck.selection.TextVersionSelector.SelectedVersion

class TextVersionSelectorSpec extends AnyFlatSpec with Matchers {

  private def makeVersion(
    date: Option[String] = Some("2024-01-15T00:00:00Z"),
    formats: Option[List[FormatDTO]] = Some(List(FormatDTO("Formatted Text", "https://example.com/text"))),
    type_ : Option[String] = Some("Introduced in House"),
  ): TextVersionDTO = TextVersionDTO(date = date, formats = formats, type_ = type_)

  "selectBestVersion" should "return None for empty list" in {
    TextVersionSelector.selectBestVersion(List.empty) shouldBe None
  }

  it should "select a single version" in {
    val versions = List(makeVersion())
    val result   = TextVersionSelector.selectBestVersion(versions)
    val _        = result shouldBe defined
    result.foreach { sv =>
      val _ = sv.url shouldBe "https://example.com/text"
      sv.formatType shouldBe "Formatted Text"
    }
  }

  it should "prefer latest date over better format priority" in {
    val versions = List(
      makeVersion(
        date = Some("2024-01-10T00:00:00Z"),
        formats = Some(List(FormatDTO("Formatted Text", "https://example.com/old-text"))),
      ),
      makeVersion(
        date = Some("2024-03-20T00:00:00Z"),
        formats = Some(List(FormatDTO("Formatted XML", "https://example.com/new-xml"))),
      ),
    )
    val result = TextVersionSelector.selectBestVersion(versions)
    val _      = result shouldBe defined
    result.foreach { sv =>
      val _ = sv.formatType shouldBe "Formatted XML"
      sv.url shouldBe "https://example.com/new-xml"
    }
  }

  it should "prefer Formatted Text over XML within same date" in {
    val sameDate = Some("2024-01-15T00:00:00Z")
    val versions = List(
      makeVersion(date = sameDate, formats = Some(List(FormatDTO("Formatted XML", "https://example.com/xml")))),
      makeVersion(date = sameDate, formats = Some(List(FormatDTO("Formatted Text", "https://example.com/text")))),
    )
    val result = TextVersionSelector.selectBestVersion(versions)
    result.foreach(_.formatType shouldBe "Formatted Text")
  }

  it should "prefer Formatted Text over PDF within same date" in {
    val sameDate = Some("2024-01-15T00:00:00Z")
    val versions = List(
      makeVersion(date = sameDate, formats = Some(List(FormatDTO("PDF", "https://example.com/pdf")))),
      makeVersion(date = sameDate, formats = Some(List(FormatDTO("Formatted Text", "https://example.com/text")))),
    )
    val result = TextVersionSelector.selectBestVersion(versions)
    result.foreach(_.formatType shouldBe "Formatted Text")
  }

  it should "prefer XML over PDF within same date" in {
    val sameDate = Some("2024-01-15T00:00:00Z")
    val versions = List(
      makeVersion(date = sameDate, formats = Some(List(FormatDTO("PDF", "https://example.com/pdf")))),
      makeVersion(date = sameDate, formats = Some(List(FormatDTO("Formatted XML", "https://example.com/xml")))),
    )
    val result = TextVersionSelector.selectBestVersion(versions)
    result.foreach(_.formatType shouldBe "Formatted XML")
  }

  it should "select latest date within same format" in {
    val versions = List(
      makeVersion(
        date = Some("2024-01-10T00:00:00Z"),
        formats = Some(List(FormatDTO("Formatted Text", "https://example.com/old"))),
      ),
      makeVersion(
        date = Some("2024-03-20T00:00:00Z"),
        formats = Some(List(FormatDTO("Formatted Text", "https://example.com/new"))),
      ),
    )
    val result = TextVersionSelector.selectBestVersion(versions)
    result.foreach(_.url shouldBe "https://example.com/new")
  }

  it should "handle version with no formats" in {
    val versions = List(makeVersion(formats = None))
    TextVersionSelector.selectBestVersion(versions) shouldBe None
  }

  it should "handle version with empty formats list" in {
    val versions = List(makeVersion(formats = Some(List.empty)))
    TextVersionSelector.selectBestVersion(versions) shouldBe None
  }

  it should "handle version with unknown format type" in {
    val versions = List(
      makeVersion(formats = Some(List(FormatDTO("Unknown Format", "https://example.com/unknown"))))
    )
    TextVersionSelector.selectBestVersion(versions) shouldBe None
  }

  it should "handle version with no date" in {
    val versions = List(makeVersion(date = None))
    val result   = TextVersionSelector.selectBestVersion(versions)
    val _        = result shouldBe defined
    result.foreach(_.date shouldBe None)
  }

  it should "prefer version with date over version without date for same format" in {
    val versions = List(
      makeVersion(
        date = None,
        formats = Some(List(FormatDTO("Formatted Text", "https://example.com/nodate"))),
      ),
      makeVersion(
        date = Some("2024-01-15T00:00:00Z"),
        formats = Some(List(FormatDTO("Formatted Text", "https://example.com/withdate"))),
      ),
    )
    val result = TextVersionSelector.selectBestVersion(versions)
    result.foreach(_.url shouldBe "https://example.com/withdate")
  }

  it should "map descriptive versionType to short code" in {
    val versions = List(makeVersion(type_ = Some("Enrolled Bill")))
    val result   = TextVersionSelector.selectBestVersion(versions)
    result.foreach(_.versionType shouldBe Some("ENR"))
  }

  it should "map Introduced in House to IH" in {
    val versions = List(makeVersion(type_ = Some("Introduced in House")))
    val result   = TextVersionSelector.selectBestVersion(versions)
    result.foreach(_.versionType shouldBe Some("IH"))
  }

  it should "map Introduced in Senate to IS" in {
    val versions = List(makeVersion(type_ = Some("Introduced in Senate")))
    val result   = TextVersionSelector.selectBestVersion(versions)
    result.foreach(_.versionType shouldBe Some("IS"))
  }

  it should "pass through unrecognized versionType unchanged" in {
    val versions = List(makeVersion(type_ = Some("Unknown Version Type")))
    val result   = TextVersionSelector.selectBestVersion(versions)
    result.foreach(_.versionType shouldBe Some("Unknown Version Type"))
  }

  it should "handle multiple formats on a single version" in {
    val version = makeVersion(formats =
      Some(
        List(
          FormatDTO("PDF", "https://example.com/pdf"),
          FormatDTO("Formatted Text", "https://example.com/text"),
          FormatDTO("Formatted XML", "https://example.com/xml"),
        )
      )
    )
    val result = TextVersionSelector.selectBestVersion(List(version))
    result.foreach(_.formatType shouldBe "Formatted Text")
  }

  it should "select PDF when it is the only recognized format" in {
    val versions = List(
      makeVersion(formats = Some(List(FormatDTO("PDF", "https://example.com/pdf"))))
    )
    val result = TextVersionSelector.selectBestVersion(versions)
    val _      = result shouldBe defined
    result.foreach { sv =>
      val _ = sv.formatType shouldBe "PDF"
      sv.url shouldBe "https://example.com/pdf"
    }
  }

  it should "ignore unknown formats and select from recognized ones" in {
    val version = makeVersion(formats =
      Some(
        List(
          FormatDTO("Unknown Format", "https://example.com/unknown"),
          FormatDTO("PDF", "https://example.com/pdf"),
        )
      )
    )
    val result = TextVersionSelector.selectBestVersion(List(version))
    result.foreach(_.formatType shouldBe "PDF")
  }

  it should "handle version with None versionType" in {
    val versions = List(makeVersion(type_ = None))
    val result   = TextVersionSelector.selectBestVersion(versions)
    val _        = result shouldBe defined
    result.foreach(_.versionType shouldBe None)
  }

}
