package repcheck.members.committees.client

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class CdirCommitteeListingParserSpec extends AnyFlatSpec with Matchers {

  private val states = Set(
    "Georgia",
    "North Carolina",
    "California",
    "Pennsylvania",
    "Massachusetts",
    "Virginia",
    "Arkansas",
    "Tennessee",
  )

  // Verbatim excerpt of the 117th-Congress CDIR HOUSECOMMITTEES granule (GovInfo), including the centered
  // single-column Chair/Vice-Chair leaders and the two-column member roster with names wrapping mid-cell.
  private val agriculture: String =
    """STANDING COMMITTEES OF THE HOUSE
      |[Democrats in roman; Republicans in italic]
      |Agriculture
      |1301 Longworth House Office Building, phone 225-2171
      |https://agriculture.house.gov
      |David Scott, of Georgia, Chair
      |Alma S. Adams, of North Carolina, Vice Chair
      |Jim Costa, of California.            Glenn Thompson, of Pennsylvania.
      |James P. McGovern, of                Austin Scott, of Georgia.
      |        Massachusetts.                Eric A. ``Rick'' Crawford, of
      |Abigail Davis Spanberger, of            Arkansas.
      |        Virginia.                     Scott DesJarlais, of Tennessee.""".stripMargin

  private def parse = CdirCommitteeListingParser.parse(agriculture, Set("Agriculture", "Appropriations"), states)

  "parse" should "extract every member of the committee across both columns and wraps" in {
    val byLast = parse.map(a => a.lastName -> a).toMap
    val _ = parse.map(_.lastName).toSet shouldBe
      Set("Scott", "Adams", "Costa", "McGovern", "Spanberger", "Thompson", "DesJarlais", "Crawford")
    // 9 assignments (two Scotts), all attributed to Agriculture, none flagged subcommittee
    val _ = parse.size shouldBe 9
    val _ = parse.map(_.committeeName).toSet shouldBe Set("Agriculture")
    byLast("Costa").firstName shouldBe "Jim"
  }

  it should "join names that wrap across lines within a column" in {
    val mcgovern = parse.find(_.lastName == "McGovern")
    val _        = mcgovern.map(_.firstName) shouldBe Some("James")
    mcgovern.map(_.state) shouldBe Some("Massachusetts")
  }

  it should "capture leadership roles from the centered, period-less leader lines" in {
    val chair = parse.find(a => a.firstName == "David" && a.lastName == "Scott")
    val _     = chair.flatMap(_.role) shouldBe Some("Chairman")
    val vice  = parse.find(_.lastName == "Adams")
    vice.flatMap(_.role) shouldBe Some("Vice Chairman")
  }

  it should "strip quoted nicknames and resolve the home state" in {
    val crawford = parse.find(_.lastName == "Crawford")
    val _        = crawford.map(_.firstName) shouldBe Some("Eric")
    crawford.map(_.state) shouldBe Some("Arkansas")
  }

  it should "not leak the previous entry's trailing state or role into the next name" in {
    val austin = parse.find(a => a.firstName == "Austin" && a.lastName == "Scott")
    val _      = austin.map(_.state) shouldBe Some("Georgia")
    // Two distinct Scotts resolved
    parse.count(_.lastName == "Scott") shouldBe 2
  }

  it should "return nothing when no known committee header anchors the text" in {
    CdirCommitteeListingParser.parse(agriculture, Set("Appropriations"), states) shouldBe empty
  }

}
