package repcheck.ingestion.votes.xml

import scala.io.Source
import scala.xml.{Elem, XML}

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import repcheck.ingestion.votes.errors.XmlParseFailed

/**
 * Unit tests for the pure [[SenateVoteXmlDecoder]]. Fixture files live under
 * `votes-pipeline/src/test/resources/senate-xml/` and carry real senate.gov XML bodies (well-formed vote document, real
 * vote-index document, hand-corrupted sample).
 *
 * Test layers (plan §"Test Layer Matrix"):
 *   - well-formed vote → `Right(SenateVoteXmlDTO)` with every DTO field populated from the fixture.
 *   - malformed vote → `Left(XmlParseFailed)` with a `rawFragment` attached so operators can triage the source.
 *   - index document → `Right(List[SenateVoteIndexEntry])` covering all entries and preserving order.
 *   - date parsing: ISO-8601 success, long-form-with-day-of-week success, long-form-without-day-of-week success
 *     (matches the real senate.gov body in the happy-path fixture), garbage → `Left`.
 */
class SenateVoteXmlDecoderSpec extends AnyFlatSpec with Matchers {

  private def loadXml(resourcePath: String): Elem = {
    val stream = getClass.getResourceAsStream(resourcePath)
    require(stream != null, s"Fixture not found: $resourcePath")
    try XML.load(stream)
    finally stream.close()
  }

  private def readRaw(resourcePath: String): String = {
    val stream = getClass.getResourceAsStream(resourcePath)
    require(stream != null, s"Fixture not found: $resourcePath")
    try Source.fromInputStream(stream, "UTF-8").mkString
    finally stream.close()
  }

  private def voteElem(
    voteDate: String,
    members: String = sampleMemberXml,
    document: String = sampleDocumentXml,
  ): Elem =
    XML.loadString(
      s"""<?xml version="1.0" encoding="UTF-8"?>
         |<roll_call_vote>
         |  <congress>119</congress>
         |  <session>1</session>
         |  <vote_number>17</vote_number>
         |  <question>On the Nomination</question>
         |  <vote_date>$voteDate</vote_date>
         |  <vote_result>Nomination Confirmed</vote_result>
         |  $document
         |  <members>$members</members>
         |</roll_call_vote>""".stripMargin
    )

  private val sampleMemberXml: String =
    """<member>
      |  <lis_member_id>S428</lis_member_id>
      |  <first_name>Angela</first_name>
      |  <last_name>Alsobrooks</last_name>
      |  <party>D</party>
      |  <state>MD</state>
      |  <vote_cast>Nay</vote_cast>
      |</member>""".stripMargin

  private val sampleDocumentXml: String =
    """<document>
      |  <document_congress>119</document_congress>
      |  <document_type>PN</document_type>
      |  <document_number>11-11</document_number>
      |  <document_name>PN11-11</document_name>
      |  <document_title>Kristi Noem, of South Dakota, to be Secretary of Homeland Security</document_title>
      |  <document_short_title/>
      |</document>""".stripMargin

  "decodeVote" should "round-trip a well-formed senate.gov vote fixture into a populated SenateVoteXmlDTO" in {
    val elem = loadXml("/senate-xml/vote_119_1_00017.xml")

    val result = SenateVoteXmlDecoder.decodeVote(elem)

    val _   = result.isRight shouldBe true
    val dto = result.toOption.getOrElse(fail("expected Right"))
    val _   = dto.congress shouldBe 119
    val _   = dto.session shouldBe 1
    val _   = dto.voteNumber shouldBe 17
    val _   = dto.question shouldBe "On the Nomination"
    val _   = dto.voteDate shouldBe "January 25, 2025, 11:30 AM"
    val _   = dto.result shouldBe "Nomination Confirmed"
    // Real fixture has 100 senators.
    val _     = dto.members.size shouldBe 100
    val first = dto.members.headOption.getOrElse(fail("expected at least one member"))
    val _     = first.lisMemberId shouldBe "S428"
    val _     = first.lastName shouldBe "Alsobrooks"
    first.voteCast shouldBe "Nay"
  }

  it should "tolerate empty <document_number>, <document_name>, and <document_title> on amendment votes (the real metadata lives in the sibling <amendment> element)" in {
    // Mirrors the actual shape senate.gov emits for amendment votes: docType is "S.Amdt."
    // and the identifying fields are self-closing empty.
    val amendmentDocument =
      """<document>
        |  <document_congress>119</document_congress>
        |  <document_type>S.Amdt.</document_type>
        |  <document_number/>
        |  <document_name/>
        |  <document_title/>
        |  <document_short_title/>
        |</document>""".stripMargin

    val elem = voteElem("2025-08-01T17:10:00", document = amendmentDocument)

    val result = SenateVoteXmlDecoder.decodeVote(elem)

    val _   = result.isRight shouldBe true
    val dto = result.toOption.getOrElse(fail("expected Right"))
    val _   = dto.document.documentType shouldBe "S.Amdt."
    val _   = dto.document.documentNumber shouldBe ""
    val _   = dto.document.documentName shouldBe ""
    dto.document.documentTitle shouldBe ""
  }

  it should "accept an ISO-8601 voteDate" in {
    val elem = voteElem("2025-04-03T14:42:00")

    val result = SenateVoteXmlDecoder.decodeVote(elem)
    val _      = result.isRight shouldBe true
    result.toOption.map(_.voteDate) shouldBe Some("2025-04-03T14:42:00")
  }

  it should "accept an ISO-8601 offset voteDate" in {
    val elem = voteElem("2025-04-03T14:42:00-04:00")

    val result = SenateVoteXmlDecoder.decodeVote(elem)
    result.toOption.map(_.voteDate) shouldBe Some("2025-04-03T14:42:00-04:00")
  }

  it should "accept a long-form voteDate with day-of-week prefix" in {
    val elem = voteElem("Thursday, April 3, 2025, 02:42 PM")

    val result = SenateVoteXmlDecoder.decodeVote(elem)
    result.toOption.map(_.voteDate) shouldBe Some("Thursday, April 3, 2025, 02:42 PM")
  }

  it should "accept a long-form voteDate without day-of-week prefix (real senate.gov format)" in {
    val elem = voteElem("January 25, 2025, 11:30 AM")

    val result = SenateVoteXmlDecoder.decodeVote(elem)
    result.toOption.map(_.voteDate) shouldBe Some("January 25, 2025, 11:30 AM")
  }

  it should "reject an unparseable voteDate and return XmlParseFailed with the raw value" in {
    val elem = voteElem("this is not a valid date at all")

    val result = SenateVoteXmlDecoder.decodeVote(elem)

    val _   = result.isLeft shouldBe true
    val err = result.left.toOption.getOrElse(fail("expected Left"))
    val _   = err shouldBe a[XmlParseFailed]
    val _   = err.detail should include("Unparseable voteDate")
    val _   = err.detail should include("this is not a valid date at all")
    err.rawFragment.getOrElse("") should include("this is not a valid date at all")
  }

  it should "reject a malformed vote fixture (root element mismatch)" in {
    // The hand-corrupted fixture truncates mid-element — XML.load would fail outright, so synthesize a
    // structurally-valid root with a bad tag to cover the "unexpected root" branch too.
    val elem = XML.loadString("<not_a_vote><congress>119</congress></not_a_vote>")

    val result = SenateVoteXmlDecoder.decodeVote(elem)

    val _   = result.isLeft shouldBe true
    val err = result.left.toOption.getOrElse(fail("expected Left"))
    err.detail should include("Expected <roll_call_vote> root")
  }

  it should "reject a missing required element with a clear message" in {
    val elem = XML.loadString(
      s"""<roll_call_vote>
        |  <congress>119</congress>
        |  <session>1</session>
        |  <vote_number>17</vote_number>
        |  <!-- <question> deliberately missing -->
        |  <vote_date>2025-04-03T14:42:00</vote_date>
        |  <vote_result>Agreed to</vote_result>
        |  $sampleDocumentXml
        |  <members/>
        |</roll_call_vote>""".stripMargin
    )

    val result = SenateVoteXmlDecoder.decodeVote(elem)

    val _   = result.isLeft shouldBe true
    val err = result.left.toOption.getOrElse(fail("expected Left"))
    err.detail should include("<question>")
  }

  it should "reject a non-integer vote_number" in {
    val elem = XML.loadString(
      s"""<roll_call_vote>
        |  <congress>119</congress>
        |  <session>1</session>
        |  <vote_number>abc</vote_number>
        |  <question>Q</question>
        |  <vote_date>2025-04-03T14:42:00</vote_date>
        |  <vote_result>X</vote_result>
        |  $sampleDocumentXml
        |  <members/>
        |</roll_call_vote>""".stripMargin
    )

    val result = SenateVoteXmlDecoder.decodeVote(elem)

    val _ = result.isLeft shouldBe true
    result.left.toOption.map(_.detail).getOrElse("") should include("vote_number")
  }

  it should "fall back to <result> when <vote_result> is missing" in {
    val elem = XML.loadString(
      s"""<roll_call_vote>
        |  <congress>119</congress>
        |  <session>1</session>
        |  <vote_number>5</vote_number>
        |  <question>Q</question>
        |  <vote_date>2025-04-03T14:42:00</vote_date>
        |  <result>Passed</result>
        |  $sampleDocumentXml
        |  <members/>
        |</roll_call_vote>""".stripMargin
    )

    val result = SenateVoteXmlDecoder.decodeVote(elem)
    result.toOption.map(_.result) shouldBe Some("Passed")
  }

  it should "fail when both <vote_result> and <result> are missing" in {
    val elem = XML.loadString(
      s"""<roll_call_vote>
        |  <congress>119</congress>
        |  <session>1</session>
        |  <vote_number>5</vote_number>
        |  <question>Q</question>
        |  <vote_date>2025-04-03T14:42:00</vote_date>
        |  $sampleDocumentXml
        |  <members/>
        |</roll_call_vote>""".stripMargin
    )

    val result = SenateVoteXmlDecoder.decodeVote(elem)

    val _ = result.isLeft shouldBe true
    result.left.toOption.map(_.detail).getOrElse("") should include("Missing <vote_result>")
  }

  it should "tolerate a missing <document> element by returning an empty document (procedural votes)" in {
    // senate.gov omits <document> for purely procedural roll-calls that don't reference a bill, nomination, or
    // treaty. Live example: 118-Senate-2 votes 129..140 ("Motion to Adjourn the Court of Impeachment Sine Die"
    // and surrounding impeachment-trial motions during the 2024 Mayorkas trial). Pre-fix every one of those was
    // dropped at decode time; post-fix they decode with an empty SenateVoteDocumentDTO whose blank documentType
    // routes through SenateVoteConverter.normalizeDocumentType into the existing NonBillOrUnknown branch and
    // persists with billId=None — same shape as amendment / nomination votes.
    val elem = XML.loadString(
      """<roll_call_vote>
        |  <congress>119</congress>
        |  <session>1</session>
        |  <vote_number>5</vote_number>
        |  <question>Q</question>
        |  <vote_date>2025-04-03T14:42:00</vote_date>
        |  <vote_result>Agreed to</vote_result>
        |  <members/>
        |</roll_call_vote>""".stripMargin
    )

    val result = SenateVoteXmlDecoder.decodeVote(elem)

    val _ = result.isRight shouldBe true
    result.fold(
      err => fail(s"decode unexpectedly failed: ${err.detail}"),
      dto => {
        val _ = dto.document.documentType shouldBe ""
        val _ = dto.document.documentNumber shouldBe ""
        val _ = dto.document.documentName shouldBe ""
        val _ = dto.document.documentTitle shouldBe ""
        val _ = dto.document.documentCongress shouldBe 0
        dto.document.documentShortTitle shouldBe None
      },
    )
  }

  it should "decode the <document> element populating every field" in {
    val elem = XML.loadString(
      s"""<roll_call_vote>
         |  <congress>119</congress>
         |  <session>1</session>
         |  <vote_number>648</vote_number>
         |  <question>On the Motion</question>
         |  <vote_date>December 17, 2025, 11:39 AM</vote_date>
         |  <vote_result>Motion Agreed to</vote_result>
         |  <document>
         |    <document_congress>119</document_congress>
         |    <document_type>S.</document_type>
         |    <document_number>1071</document_number>
         |    <document_name>S. 1071</document_name>
         |    <document_title>A bill to require the Secretary of Veterans Affairs to disinter the remains of Fernando V. Cota.</document_title>
         |    <document_short_title/>
         |  </document>
         |  <members>$sampleMemberXml</members>
         |</roll_call_vote>""".stripMargin
    )

    val result = SenateVoteXmlDecoder.decodeVote(elem)
    val dto    = result.toOption.getOrElse(fail("expected Right"))

    val _ = dto.document.documentCongress shouldBe 119
    val _ = dto.document.documentType shouldBe "S."
    val _ = dto.document.documentNumber shouldBe "1071"
    val _ = dto.document.documentName shouldBe "S. 1071"
    val _ =
      dto.document.documentTitle should include("A bill to require the Secretary of Veterans Affairs")
    dto.document.documentShortTitle shouldBe None
  }

  it should "decode the <document> element with a populated documentShortTitle" in {
    val elem = XML.loadString(
      s"""<roll_call_vote>
         |  <congress>117</congress>
         |  <session>1</session>
         |  <vote_number>50</vote_number>
         |  <question>On Passage</question>
         |  <vote_date>2021-03-06T00:00:00</vote_date>
         |  <vote_result>Agreed to</vote_result>
         |  <document>
         |    <document_congress>117</document_congress>
         |    <document_type>H.R.</document_type>
         |    <document_number>1319</document_number>
         |    <document_name>H.R. 1319</document_name>
         |    <document_title>American Rescue Plan Act of 2021</document_title>
         |    <document_short_title>ARPA</document_short_title>
         |  </document>
         |  <members>$sampleMemberXml</members>
         |</roll_call_vote>""".stripMargin
    )

    val result = SenateVoteXmlDecoder.decodeVote(elem)
    val dto    = result.toOption.getOrElse(fail("expected Right"))

    dto.document.documentShortTitle shouldBe Some("ARPA")
  }

  it should "fail the entire decode when a member entry is missing a required field" in {
    val bad =
      """<member>
        |  <!-- missing <lis_member_id> -->
        |  <first_name>Angela</first_name>
        |  <last_name>Alsobrooks</last_name>
        |  <party>D</party>
        |  <state>MD</state>
        |  <vote_cast>Nay</vote_cast>
        |</member>""".stripMargin
    val elem = voteElem("2025-04-03T14:42:00", members = bad)

    val result = SenateVoteXmlDecoder.decodeVote(elem)

    val _ = result.isLeft shouldBe true
    result.left.toOption.map(_.detail).getOrElse("") should include("<lis_member_id>")
  }

  it should "decode a vote with zero members (empty <members/>) as an empty position list" in {
    val elem = voteElem("2025-04-03T14:42:00", members = "")

    val result = SenateVoteXmlDecoder.decodeVote(elem)
    result.toOption.map(_.members) shouldBe Some(List.empty)
  }

  it should "truncate rawFragment in XmlParseFailed to avoid dumping full bodies" in {
    val bigBody = "<not_a_vote>" + ("<junk>x</junk>" * 50) + "</not_a_vote>"
    val elem    = XML.loadString(bigBody)

    val result = SenateVoteXmlDecoder.decodeVote(elem)
    val _      = result.isLeft shouldBe true
    val frag   = result.left.toOption.flatMap(_.rawFragment).getOrElse("")
    frag.length should be <= 210 // 200 + "..." + small slack
  }

  "decodeIndex" should "round-trip a real vote_menu fixture into a populated list of entries" in {
    val elem = loadXml("/senate-xml/vote_menu_119_1.xml")

    val result = SenateVoteXmlDecoder.decodeIndex(elem)

    val entries = result match {
      case Right(list) => list
      case Left(err)   => fail(s"expected Right, got Left(detail=${err.detail})")
    }
    // The session 119-1 menu fixture contains many entries; assert we decoded a non-trivial count and each entry is
    // populated rather than asserting an exact length (which would couple the test to senate.gov's ongoing updates
    // if the fixture were ever refreshed).
    val _ = entries.size should be >= 50
    // First entry in the menu is vote 00659 (real fixture — highest roll-call number first).
    val first = entries.headOption.getOrElse(fail("expected at least one entry"))
    val _     = first.voteNumber shouldBe 659
    val _     = first.voteDate shouldBe "18-Dec"
    val _     = first.question should include("Cloture Motion")
    first.result shouldBe "Agreed to"
  }

  it should "reject a malformed index document (wrong root element)" in {
    val elem = XML.loadString("<not_an_index></not_an_index>")

    val result = SenateVoteXmlDecoder.decodeIndex(elem)

    val _ = result.isLeft shouldBe true
    result.left.toOption.map(_.detail).getOrElse("") should include("Expected <vote_summary> root")
  }

  it should "silently skip a vote entry with a non-numeric <vote_number> rather than failing the whole index" in {
    // Per-entry tolerance: a single malformed entry no longer aborts the entire session's index parse.
    // The bad entry is dropped; any siblings continue to decode normally. Surfaced live during P6
    // backfill where one bad entry was killing 200+ siblings in older Senate index XMLs.
    val elem = XML.loadString(
      """<vote_summary>
        |  <votes>
        |    <vote>
        |      <vote_number>bad</vote_number>
        |      <vote_date>1-Jan</vote_date>
        |      <question>Q</question>
        |      <result>R</result>
        |    </vote>
        |    <vote>
        |      <vote_number>00100</vote_number>
        |      <vote_date>2-Jan</vote_date>
        |      <question>Good Q</question>
        |      <result>Agreed</result>
        |    </vote>
        |  </votes>
        |</vote_summary>""".stripMargin
    )

    val result  = SenateVoteXmlDecoder.decodeIndex(elem)
    val entries = result.toOption.getOrElse(fail("expected Right with the surviving entry"))
    val _       = entries.size shouldBe 1
    entries.headOption.map(_.voteNumber) shouldBe Some(100)
  }

  it should "produce an empty list when the index has no <vote> entries" in {
    val elem = XML.loadString("<vote_summary><votes/></vote_summary>")

    val result = SenateVoteXmlDecoder.decodeIndex(elem)
    result shouldBe Right(List.empty)
  }

  it should "silently skip a non-en-bloc entry that is missing <question> rather than failing the whole index" in {
    val elem = XML.loadString(
      """<vote_summary>
        |  <votes>
        |    <vote>
        |      <vote_number>00124</vote_number>
        |      <vote_date>1-Jan</vote_date>
        |      <!-- question missing, no en_bloc -->
        |      <result>Agreed</result>
        |    </vote>
        |    <vote>
        |      <vote_number>00125</vote_number>
        |      <vote_date>2-Jan</vote_date>
        |      <question>Good Q</question>
        |      <result>Agreed</result>
        |    </vote>
        |  </votes>
        |</vote_summary>""".stripMargin
    )

    val result  = SenateVoteXmlDecoder.decodeIndex(elem)
    val entries = result.toOption.getOrElse(fail("expected Right with the surviving entry"))
    val _       = entries.size shouldBe 1
    entries.headOption.map(_.voteNumber) shouldBe Some(125)
  }

  it should "treat a direct <question> child that is only whitespace as missing (en_bloc fallback)" in {
    val elem = XML.loadString(
      """<vote_summary>
        |  <votes>
        |    <vote>
        |      <vote_number>00125</vote_number>
        |      <vote_date>1-Jan</vote_date>
        |      <question>   </question>
        |      <result>Agreed</result>
        |      <en_bloc><matter><issue>X</issue></matter></en_bloc>
        |    </vote>
        |  </votes>
        |</vote_summary>""".stripMargin
    )

    val result = SenateVoteXmlDecoder.decodeIndex(elem)
    result.toOption.flatMap(_.headOption).map(_.question) shouldBe Some("En Bloc")
  }

  it should "substitute 'En Bloc' for missing <question>/<result> when the vote is an en_bloc batch" in {
    val elem = XML.loadString(
      """<vote_summary>
        |  <votes>
        |    <vote>
        |      <vote_number>00655</vote_number>
        |      <vote_date>18-Dec</vote_date>
        |      <en_bloc>
        |        <matter>
        |          <issue>PN416-9</issue>
        |          <question>On the Nomination</question>
        |          <result>Confirmed</result>
        |        </matter>
        |      </en_bloc>
        |    </vote>
        |  </votes>
        |</vote_summary>""".stripMargin
    )

    val result  = SenateVoteXmlDecoder.decodeIndex(elem)
    val entries = result.toOption.getOrElse(fail("expected Right"))
    val _       = entries.size shouldBe 1
    val entry   = entries.headOption.getOrElse(fail("expected one entry"))
    val _       = entry.voteNumber shouldBe 655
    val _       = entry.question shouldBe "En Bloc"
    entry.result shouldBe "En Bloc"
  }

  it should "silently skip a non-en-bloc entry that is missing <result> rather than failing the whole index" in {
    val elem = XML.loadString(
      """<vote_summary>
        |  <votes>
        |    <vote>
        |      <vote_number>00123</vote_number>
        |      <vote_date>1-Jan</vote_date>
        |      <question>Q</question>
        |      <!-- result missing, no en_bloc -->
        |    </vote>
        |    <vote>
        |      <vote_number>00124</vote_number>
        |      <vote_date>2-Jan</vote_date>
        |      <question>Q</question>
        |      <result>Agreed</result>
        |    </vote>
        |  </votes>
        |</vote_summary>""".stripMargin
    )

    val result  = SenateVoteXmlDecoder.decodeIndex(elem)
    val entries = result.toOption.getOrElse(fail("expected Right with the surviving entry"))
    val _       = entries.size shouldBe 1
    entries.headOption.map(_.voteNumber) shouldBe Some(124)
  }

  it should "preserve <vote> decoding order in the returned list" in {
    val elem = XML.loadString(
      """<vote_summary>
        |  <votes>
        |    <vote><vote_number>00010</vote_number><vote_date>1-Jan</vote_date><question>Q1</question><result>R1</result></vote>
        |    <vote><vote_number>00020</vote_number><vote_date>2-Jan</vote_date><question>Q2</question><result>R2</result></vote>
        |    <vote><vote_number>00030</vote_number><vote_date>3-Jan</vote_date><question>Q3</question><result>R3</result></vote>
        |  </votes>
        |</vote_summary>""".stripMargin
    )

    val result = SenateVoteXmlDecoder.decodeIndex(elem)
    result.toOption.map(_.map(_.voteNumber)) shouldBe Some(List(10, 20, 30))
  }

  it should "load the hand-corrupted fixture body but produce a structural failure" in {
    // The malformed fixture is not well-formed XML (intentionally truncated mid-element), so XML.load blows up at
    // parse time rather than reaching the decoder. This guards the fixture's role: verifies that the corrupted body
    // is surfaced as a parse-level failure via scala-xml before the decoder sees it.
    val raw = readRaw("/senate-xml/vote_malformed.xml")

    an[Exception] shouldBe thrownBy {
      val _ = XML.loadString(raw)
    }
  }

  it should "locate the malformed fixture on the classpath" in {
    // The fixture file is present; we just can't load it as well-formed XML.
    Option(getClass.getResourceAsStream("/senate-xml/vote_malformed.xml")).map(_.available()).getOrElse(0) should be > 0
  }

  // ------------------------------------------------------------------
  // §7.4 — decodeVote: amendment-typed top-level fields are folded into document DTO
  // ------------------------------------------------------------------

  /** Build a `<roll_call_vote>` element with arbitrary extra top-level XML appended. */
  private def voteElemWithExtra(extraTopLevel: String, document: String = sampleDocumentXml): Elem =
    XML.loadString(
      s"""<?xml version="1.0" encoding="UTF-8"?>
         |<roll_call_vote>
         |  <congress>117</congress>
         |  <session>1</session>
         |  <vote_number>312</vote_number>
         |  <question>On the Amendment</question>
         |  <vote_date>2025-01-25T11:30:00</vote_date>
         |  <vote_result>Amendment Agreed To</vote_result>
         |  $document
         |  <members>$sampleMemberXml</members>
         |  $extraTopLevel
         |</roll_call_vote>""".stripMargin
    )

  "decodeVote" should "extract amendmentNumber, amendmentToDocumentNumber, and amendmentToDocumentShortTitle into the document DTO when present" in {
    val extra =
      """<amendment_number>S.Amdt. 2137</amendment_number>
        |<amendment_to_document_number>H.R. 3684</amendment_to_document_number>
        |<amendment_to_document_short_title>INVEST in America Act</amendment_to_document_short_title>""".stripMargin

    val result = SenateVoteXmlDecoder.decodeVote(voteElemWithExtra(extra))

    val _   = result.isRight shouldBe true
    val dto = result.toOption.getOrElse(fail("expected Right"))
    val _   = dto.document.amendmentNumber shouldBe Some("S.Amdt. 2137")
    val _   = dto.document.amendmentToDocumentNumber shouldBe Some("H.R. 3684")
    dto.document.amendmentToDocumentShortTitle shouldBe Some("INVEST in America Act")
  }

  it should "return None for amendment fields on the document DTO when they're missing entirely (bill-vote XML)" in {
    val result = SenateVoteXmlDecoder.decodeVote(voteElemWithExtra(""))

    val _   = result.isRight shouldBe true
    val dto = result.toOption.getOrElse(fail("expected Right"))
    val _   = dto.document.amendmentNumber shouldBe None
    val _   = dto.document.amendmentToDocumentNumber shouldBe None
    dto.document.amendmentToDocumentShortTitle shouldBe None
  }

  it should "return None for amendment fields on the document DTO when they're self-closing empty elements" in {
    val extra =
      """<amendment_number/>
        |<amendment_to_document_number/>
        |<amendment_to_document_short_title/>""".stripMargin

    val result = SenateVoteXmlDecoder.decodeVote(voteElemWithExtra(extra))

    val _   = result.isRight shouldBe true
    val dto = result.toOption.getOrElse(fail("expected Right"))
    val _   = dto.document.amendmentNumber shouldBe None
    val _   = dto.document.amendmentToDocumentNumber shouldBe None
    dto.document.amendmentToDocumentShortTitle shouldBe None
  }

}
