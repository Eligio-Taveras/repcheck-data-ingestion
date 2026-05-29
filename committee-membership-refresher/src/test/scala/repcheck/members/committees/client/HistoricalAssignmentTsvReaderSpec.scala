package repcheck.members.committees.client

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class HistoricalAssignmentTsvReaderSpec extends AnyFlatSpec with Matchers {

  private def row(
    congress: String = "117",
    chamber: String = "Senate",
    code: String = "SSJU00",
    name: String = "Judiciary Committee",
    cType: String = "Standing",
    bioguide: String = "B001",
    role: String = "Chairman",
    rank: String = "1",
  ): String =
    List(congress, chamber, code, name, cType, bioguide, role, rank).mkString("\t")

  "isHeader" should "recognize the canonical header" in {
    HistoricalAssignmentTsvReader.isHeader(HistoricalAssignmentTsvReader.Header) shouldBe true
  }

  it should "not treat a data row as the header" in {
    HistoricalAssignmentTsvReader.isHeader(row()) shouldBe false
  }

  "parseLine" should "parse a fully populated row" in {
    val result = HistoricalAssignmentTsvReader.parseLine(row())
    val _      = result.isRight shouldBe true
    result.foreach { r =>
      val _ = r.congress shouldBe 117
      val _ = r.chamber shouldBe "Senate"
      val _ = r.committeeCode shouldBe "SSJU00"
      val _ = r.committeeName shouldBe Some("Judiciary Committee")
      val _ = r.committeeType shouldBe Some("Standing")
      val _ = r.bioguideId shouldBe "B001"
      val _ = r.role shouldBe Some("Chairman")
      r.rank shouldBe Some(1)
    }
  }

  it should "treat blank optional columns as None" in {
    val result = HistoricalAssignmentTsvReader.parseLine(row(name = "", cType = "", role = "", rank = ""))
    val _      = result.isRight shouldBe true
    result.foreach { r =>
      val _ = r.committeeName shouldBe None
      val _ = r.committeeType shouldBe None
      val _ = r.role shouldBe None
      r.rank shouldBe None
    }
  }

  it should "preserve commas in committee names (tab-separated)" in {
    val result = HistoricalAssignmentTsvReader.parseLine(
      row(name = "Banking, Housing, and Urban Affairs Committee")
    )
    result.map(_.committeeName) shouldBe Right(Some("Banking, Housing, and Urban Affairs Committee"))
  }

  it should "reject a row with the wrong column count" in {
    val result = HistoricalAssignmentTsvReader.parseLine("117\tSenate\tSSJU00")
    result.isLeft shouldBe true
  }

  it should "reject a non-numeric congress" in {
    HistoricalAssignmentTsvReader.parseLine(row(congress = "CXVII")).isLeft shouldBe true
  }

  it should "reject a blank chamber" in {
    HistoricalAssignmentTsvReader.parseLine(row(chamber = "")).isLeft shouldBe true
  }

  it should "reject a blank committee_code" in {
    HistoricalAssignmentTsvReader.parseLine(row(code = "")).isLeft shouldBe true
  }

  it should "reject a blank bioguide_id" in {
    HistoricalAssignmentTsvReader.parseLine(row(bioguide = "")).isLeft shouldBe true
  }

  it should "reject a non-numeric rank" in {
    HistoricalAssignmentTsvReader.parseLine(row(rank = "first")).isLeft shouldBe true
  }

}
