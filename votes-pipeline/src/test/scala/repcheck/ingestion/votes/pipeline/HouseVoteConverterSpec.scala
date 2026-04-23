package repcheck.ingestion.votes.pipeline

import cats.effect.IO
import cats.effect.unsafe.implicits.global

import org.mockito.ArgumentMatchers.{any, anyString}
import org.mockito.Mockito.when
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar
import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}
import repcheck.ingestion.votes.errors.VoteConversionFailed
import repcheck.shared.models.congress.dto.vote.{VoteMembersDTO, VoteResultDTO}

/**
 * Unit spec for [[HouseVoteConverter]]. The converter is pure (aside from logging on the error path) — it wraps
 * [[repcheck.shared.models.congress.dto.conversions.VoteConversions.VoteMembersDTOOps.toDO]] with a no-op bill lookup
 * and returns the resulting [[repcheck.shared.models.congress.dos.results.VoteConversionResult]] directly. Member and
 * bill resolution, plus `VotePositionDO` materialization, happen in the processor.
 */
class HouseVoteConverterSpec extends AnyFlatSpec with Matchers with MockitoSugar {

  private val logCtx = LogContext(runId = "r", stepName = "test")

  private def mkLogger: PipelineLogger[IO] = {
    val m = mock[PipelineLogger[IO]]
    when(m.info(any[LogContext], anyString())).thenReturn(IO.unit)
    when(m.warn(any[LogContext], anyString())).thenReturn(IO.unit)
    when(m.error(any[LogContext], anyString(), any[Option[Throwable]])).thenReturn(IO.unit)
    m
  }

  private def houseDto(
    rollCall: Int = 42,
    congress: Int = 119,
    session: Int = 1,
    question: Option[String] = Some("On Passage"),
    legislationType: Option[String] = Some("HR"),
    legislationNumber: Option[String] = Some("1234"),
    updateDate: Option[String] = Some("2024-06-01T12:00:00Z"),
    positions: List[VoteResultDTO] = List.empty,
  ): VoteMembersDTO =
    VoteMembersDTO(
      congress = congress,
      chamber = "House",
      rollCallNumber = rollCall,
      sessionNumber = Some(session),
      startDate = Some("2024-06-01T12:00:00Z"),
      updateDate = updateDate,
      result = Some("Passed"),
      voteType = Some("Yea-and-Nay"),
      legislationNumber = legislationNumber,
      legislationType = legislationType,
      legislationUrl = Some("https://congress.gov/bill/119/hr/1234"),
      url = Some("https://api.congress.gov/v3/house-vote/119/1/42"),
      identifier = None,
      sourceDataUrl = Some("https://api.congress.gov/v3/house-vote/119/1/42"),
      voteQuestion = question,
      results = Some(positions),
    )

  private def posDto(bioguide: String, cast: String = "Yea"): VoteResultDTO =
    VoteResultDTO(
      memberId = Some(bioguide),
      firstName = Some("First"),
      lastName = Some("Last"),
      voteCast = Some(cast),
      party = Some("D"),
      state = Some("NY"),
    )

  // ------------------------------------------------------------------
  // Happy paths
  // ------------------------------------------------------------------

  "convert" should "produce VoteConversionResult with billId=None (processor resolves) and positions with Left(bioguide)" in {
    val converter = new HouseVoteConverter[IO](mkLogger)
    val dto       = houseDto(positions = List(posDto("A000055", "Yea")))

    val result = converter.convert(dto, logCtx).unsafeRunSync()

    val _ = result.vote.naturalKey shouldBe "119-House-1-42"
    // Converter does NOT resolve bills — that's the processor's job. billId comes back None from the no-op lookup.
    val _ = result.vote.billId shouldBe None
    // Conversion DID extract the bill natural key for downstream use.
    val _ = result.billNaturalKey shouldBe Some("119-HR-1234")
    // Positions are unresolved (memberSource = Left(bioguide))
    val _     = result.positions.length shouldBe 1
    val first = result.positions.headOption.getOrElse(fail("expected one position"))
    first.memberSource shouldBe Left("A000055")
  }

  it should "leave billNaturalKey = None for a procedural vote (no legislationType/number)" in {
    val converter = new HouseVoteConverter[IO](mkLogger)
    val dto       = houseDto(legislationType = None, legislationNumber = None, positions = List(posDto("A000055")))

    val result = converter.convert(dto, logCtx).unsafeRunSync()

    val _ = result.vote.billId shouldBe None
    result.billNaturalKey shouldBe None
  }

  // ------------------------------------------------------------------
  // Failure paths
  // ------------------------------------------------------------------

  it should "raise VoteConversionFailed when validation returns Left (e.g., invalid congress)" in {
    val converter = new HouseVoteConverter[IO](mkLogger)
    val dto       = houseDto(congress = 0)

    val outcome = converter.convert(dto, logCtx).attempt.unsafeRunSync()

    outcome match {
      case Left(e: VoteConversionFailed) =>
        e.getMessage should include("Conversion failed for vote")
      case other => fail(s"expected Left(VoteConversionFailed), got $other")
    }
  }

  // Theme 6: directed test for the previously-uncovered error-logging branch on Left
  it should "log the error at error level when raising VoteConversionFailed" in {
    val loggerMock = mkLogger
    val converter  = new HouseVoteConverter[IO](loggerMock)
    val dto        = houseDto(congress = -1)

    val _ = converter.convert(dto, logCtx).attempt.unsafeRunSync()

    import org.mockito.Mockito.{times, verify}
    verify(loggerMock, times(1)).error(any[LogContext], anyString(), any[Option[Throwable]])
  }

  // ------------------------------------------------------------------
  // buildNaturalKey helper
  // ------------------------------------------------------------------

  "buildNaturalKey" should "match VoteConversions.buildVoteNaturalKey formatting" in {
    val converter = new HouseVoteConverter[IO](mkLogger)
    val dto       = houseDto(rollCall = 17, congress = 119, session = 1)
    converter.buildNaturalKey(dto) shouldBe "119-House-1-17"
  }

  it should "use session = 0 when the DTO's sessionNumber is None" in {
    val converter = new HouseVoteConverter[IO](mkLogger)
    val dto       = houseDto().copy(sessionNumber = None)
    converter.buildNaturalKey(dto) shouldBe "119-House-0-42"
  }

}
