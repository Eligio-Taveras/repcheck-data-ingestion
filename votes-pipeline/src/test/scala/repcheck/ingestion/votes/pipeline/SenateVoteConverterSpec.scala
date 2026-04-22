package repcheck.ingestion.votes.pipeline

import java.time.LocalDate

import cats.effect.IO
import cats.effect.unsafe.implicits.global

import org.mockito.ArgumentMatchers.{any, anyString}
import org.mockito.Mockito.when
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar
import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}
import repcheck.ingestion.votes.errors.VoteConversionFailed
import repcheck.shared.models.congress.common.{Chamber, Party, UsState}
import repcheck.shared.models.congress.dto.vote.{SenateVoteMemberXmlDTO, SenateVoteXmlDTO}
import repcheck.shared.models.congress.vote.{VoteCast, VoteType}

class SenateVoteConverterSpec extends AnyFlatSpec with Matchers with MockitoSugar {

  private val logCtx = LogContext(runId = "r", stepName = "test")

  private def mkLogger: PipelineLogger[IO] = {
    val m = mock[PipelineLogger[IO]]
    when(m.info(any[LogContext], anyString())).thenReturn(IO.unit)
    when(m.warn(any[LogContext], anyString())).thenReturn(IO.unit)
    when(m.error(any[LogContext], anyString(), any[Option[Throwable]])).thenReturn(IO.unit)
    m
  }

  private def senator(
    lisId: String,
    firstName: String = "Angela",
    lastName: String = "Alsobrooks",
    party: String = "D",
    state: String = "MD",
    voteCast: String = "Yea",
  ): SenateVoteMemberXmlDTO =
    SenateVoteMemberXmlDTO(
      lisMemberId = lisId,
      firstName = firstName,
      lastName = lastName,
      party = party,
      state = state,
      voteCast = voteCast,
    )

  private def senateDto(
    congress: Int = 119,
    session: Int = 1,
    voteNumber: Int = 17,
    question: String = "On Passage of the Bill",
    voteDate: String = "January 25, 2025, 11:30 AM",
    result: String = "Bill Passed",
    members: List[SenateVoteMemberXmlDTO] = List.empty,
  ): SenateVoteXmlDTO =
    SenateVoteXmlDTO(
      congress = congress,
      session = session,
      voteNumber = voteNumber,
      question = question,
      voteDate = voteDate,
      result = result,
      members = members,
    )

  // ------------------------------------------------------------------
  // Happy paths
  // ------------------------------------------------------------------

  "convert" should "produce a Senate VoteDO + dual-identity VotePositionDO list when every senator is mapped" in {
    val converter = new SenateVoteConverter[IO](mkLogger)
    val dto       = senateDto(members = List(senator("S428", voteCast = "Yea"), senator("S429", voteCast = "Nay")))
    val lisMap    = Map("S428" -> 1L, "S429" -> 2L)

    val (voteDo, positions) = converter.convert(dto, lisMap, logCtx).unsafeRunSync()

    val _ = voteDo.naturalKey shouldBe "119-Senate-1-17"
    val _ = voteDo.chamber shouldBe Chamber.Senate
    val _ = voteDo.billId shouldBe None               // senate.gov XML has no bill linkage
    val _ = voteDo.sessionNumber shouldBe Some(1)
    val _ = voteDo.rollNumber shouldBe 17
    val _ = voteDo.question shouldBe Some("On Passage of the Bill")
    val _ = voteDo.voteType shouldBe Some(VoteType.Passage)
    val _ = voteDo.voteDate shouldBe Some(LocalDate.parse("2025-01-25"))
    val _ = voteDo.updateDate.isDefined shouldBe true // derived from voteDate at 00:00 UTC
    val _ = positions.length shouldBe 2
    val a = positions.find(_.lisMemberId.contains(1L)).getOrElse(fail("expected position for lisId 1"))
    val _ = a.memberId shouldBe None
    val _ = a.lisMemberId shouldBe Some(1L)
    val _ = a.position shouldBe Some(VoteCast.Yea)
    val _ = a.partyAtVote shouldBe Some(Party.Democrat)
    val _ = a.stateAtVote shouldBe Some(UsState.Maryland)
    val b = positions.find(_.lisMemberId.contains(2L)).getOrElse(fail("expected position for lisId 2"))
    b.position shouldBe Some(VoteCast.Nay)
  }

  it should "produce an empty positions list when dto.members is empty" in {
    val converter = new SenateVoteConverter[IO](mkLogger)
    val dto       = senateDto()
    val lisMap    = Map.empty[String, Long]

    val (voteDo, positions) = converter.convert(dto, lisMap, logCtx).unsafeRunSync()

    val _ = voteDo.naturalKey shouldBe "119-Senate-1-17"
    positions shouldBe List.empty
  }

  it should "raise VoteConversionFailed when any senator's LIS id is missing from the map" in {
    val converter = new SenateVoteConverter[IO](mkLogger)
    val dto       = senateDto(members = List(senator("S428"), senator("S429")))
    val lisMap    = Map("S428" -> 1L) // S429 missing

    val outcome = converter.convert(dto, lisMap, logCtx).attempt.unsafeRunSync()

    outcome match {
      case Left(e: VoteConversionFailed) =>
        e.getMessage should include("S429")
      case other => fail(s"expected Left(VoteConversionFailed), got $other")
    }
  }

  // ------------------------------------------------------------------
  // VoteType classification
  // ------------------------------------------------------------------

  "VoteType classification" should "classify 'On the Cloture Motion' as Cloture" in {
    val converter   = new SenateVoteConverter[IO](mkLogger)
    val dto         = senateDto(question = "On the Cloture Motion")
    val (voteDo, _) = converter.convert(dto, Map.empty, logCtx).unsafeRunSync()
    voteDo.voteType shouldBe Some(VoteType.Cloture)
  }

  it should "classify 'On the Nomination' via the Other fallback" in {
    val converter   = new SenateVoteConverter[IO](mkLogger)
    val dto         = senateDto(question = "On the Nomination")
    val (voteDo, _) = converter.convert(dto, Map.empty, logCtx).unsafeRunSync()
    // "On the Nomination" doesn't match any specific bucket; falls through to Other
    voteDo.voteType shouldBe Some(VoteType.Other)
  }

  // ------------------------------------------------------------------
  // parseVoteDate format variants
  // ------------------------------------------------------------------

  "parseVoteDate" should "parse 'January 25, 2025, 11:30 AM' (long form no day-of-week)" in {
    SenateVoteConverter.parseVoteDate("January 25, 2025, 11:30 AM") shouldBe Some(
      LocalDate.parse("2025-01-25")
    )
  }

  it should "parse 'Thursday, April 3, 2025, 02:42 PM' (long form with day-of-week)" in {
    SenateVoteConverter.parseVoteDate("Thursday, April 3, 2025, 02:42 PM") shouldBe Some(
      LocalDate.parse("2025-04-03")
    )
  }

  it should "parse ISO-offset '2025-01-25T11:30:00Z' date-times" in {
    SenateVoteConverter.parseVoteDate("2025-01-25T11:30:00Z") shouldBe Some(
      LocalDate.parse("2025-01-25")
    )
  }

  it should "tolerate extra interior whitespace" in {
    SenateVoteConverter.parseVoteDate("January  25,  2025,  11:30 AM") shouldBe Some(
      LocalDate.parse("2025-01-25")
    )
  }

  it should "return None for unparseable inputs (decoder already filters these upstream)" in {
    SenateVoteConverter.parseVoteDate("not a date") shouldBe None
  }

}
