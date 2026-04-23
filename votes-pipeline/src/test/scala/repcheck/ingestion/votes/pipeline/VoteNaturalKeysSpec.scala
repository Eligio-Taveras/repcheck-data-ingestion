package repcheck.ingestion.votes.pipeline

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import repcheck.ingestion.votes.xml.SenateVoteIndexEntry
import repcheck.shared.models.congress.dto.vote.{
  SenateVoteDocumentDTO,
  SenateVoteXmlDTO,
  VoteListItemDTO,
  VoteMembersDTO,
}

/**
 * Unit spec for the pure natural-key helpers extracted into [[VoteNaturalKeys]]. Covers both vote and bill key
 * formatting for each chamber.
 */
class VoteNaturalKeysSpec extends AnyFlatSpec with Matchers {

  private def houseListItem(
    rollCall: Int,
    sessionNumber: Option[Int],
    congress: Int = 119,
  ): VoteListItemDTO =
    VoteListItemDTO(
      congress = congress,
      chamber = "House",
      rollCallNumber = rollCall,
      sessionNumber = sessionNumber,
      startDate = None,
      updateDate = None,
      result = None,
      voteType = None,
      legislationNumber = None,
      legislationType = None,
      legislationUrl = None,
      url = None,
      identifier = None,
      sourceDataUrl = None,
    )

  private def houseMembers(
    legislationType: Option[String],
    legislationNumber: Option[String],
    congress: Int = 119,
  ): VoteMembersDTO =
    VoteMembersDTO(
      congress = congress,
      chamber = "House",
      rollCallNumber = 42,
      sessionNumber = Some(1),
      startDate = None,
      updateDate = None,
      result = None,
      voteType = None,
      legislationNumber = legislationNumber,
      legislationType = legislationType,
      legislationUrl = None,
      url = None,
      identifier = None,
      sourceDataUrl = None,
      voteQuestion = None,
      results = Some(List.empty),
    )

  private def senateEntry(voteNumber: Int): SenateVoteIndexEntry =
    SenateVoteIndexEntry(voteNumber = voteNumber, voteDate = "Jan 25", question = "On Passage", result = "Passed")

  private def senateXml(
    docType: String,
    docNumber: String,
    congress: Int = 119,
    session: Int = 1,
    voteNumber: Int = 17,
    docCongress: Int = 119,
  ): SenateVoteXmlDTO =
    SenateVoteXmlDTO(
      congress = congress,
      session = session,
      voteNumber = voteNumber,
      question = "Q",
      voteDate = "January 25, 2025, 11:30 AM",
      result = "Passed",
      document = SenateVoteDocumentDTO(
        documentCongress = docCongress,
        documentType = docType,
        documentNumber = docNumber,
        documentName = s"$docType $docNumber",
        documentTitle = "Title",
        documentShortTitle = None,
      ),
      members = List.empty,
    )

  // ------------------------------------------------------------------
  // houseVote
  // ------------------------------------------------------------------

  "houseVote" should "use the DTO's sessionNumber when present" in {
    VoteNaturalKeys.houseVote(houseListItem(rollCall = 5, sessionNumber = Some(2)), fallbackSession = 99) shouldBe
      "119-House-2-5"
  }

  it should "fall back to the configured session when the DTO's sessionNumber is None" in {
    VoteNaturalKeys.houseVote(houseListItem(rollCall = 5, sessionNumber = None), fallbackSession = 7) shouldBe
      "119-House-7-5"
  }

  // ------------------------------------------------------------------
  // senateVote
  // ------------------------------------------------------------------

  "senateVote" should "build the natural key from the threaded (congress, session)" in {
    VoteNaturalKeys.senateVote(senateEntry(voteNumber = 17), congress = 119, session = 1) shouldBe
      "119-Senate-1-17"
  }

  it should "accept an arbitrary vote number and congress/session" in {
    VoteNaturalKeys.senateVote(senateEntry(voteNumber = 648), congress = 118, session = 2) shouldBe
      "118-Senate-2-648"
  }

  // ------------------------------------------------------------------
  // houseBill
  // ------------------------------------------------------------------

  "houseBill" should "derive the bill key from legislationType + legislationNumber + congress" in {
    VoteNaturalKeys.houseBill(
      houseMembers(congress = 119, legislationType = Some("hr"), legislationNumber = Some("30"))
    ) shouldBe
      Some("119-HR-30")
  }

  it should "upper-case the legislationType regardless of input casing" in {
    VoteNaturalKeys.houseBill(houseMembers(legislationType = Some("sjres"), legislationNumber = Some("5"))) shouldBe
      Some("119-SJRES-5")
  }

  it should "return None when legislationType is missing" in {
    VoteNaturalKeys.houseBill(houseMembers(legislationType = None, legislationNumber = Some("5"))) shouldBe None
  }

  it should "return None when legislationNumber is missing" in {
    VoteNaturalKeys.houseBill(houseMembers(legislationType = Some("hr"), legislationNumber = None)) shouldBe None
  }

  // ------------------------------------------------------------------
  // senateBill
  // ------------------------------------------------------------------

  "senateBill" should "always return None (the Senate converter's classifyDocument is authoritative)" in {
    val _ = VoteNaturalKeys.senateBill(senateXml(docType = "S.", docNumber = "1071")) shouldBe None
    VoteNaturalKeys.senateBill(senateXml(docType = "PN", docNumber = "11-11")) shouldBe None
  }

}
