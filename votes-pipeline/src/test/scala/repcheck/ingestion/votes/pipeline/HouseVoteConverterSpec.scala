package repcheck.ingestion.votes.pipeline

import cats.effect.IO
import cats.effect.unsafe.implicits.global

import org.mockito.ArgumentMatchers.{any, anyString, eq => eqTo}
import org.mockito.Mockito.{never, times, verify, when}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar
import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}
import repcheck.ingestion.votes.errors.VoteConversionFailed
import repcheck.shared.models.congress.common.Chamber
import repcheck.shared.models.congress.dto.vote.{VoteMembersDTO, VoteResultDTO}

/**
 * Unit spec for [[HouseVoteConverter]]. Both resolvers are mocked via MockitoSugar so the spec exercises the converter
 * in isolation — bill lookup, member batch resolution, and the materialization of `VotePositionDO` rows from the
 * `VoteConversionResult.positions` list. The converter delegates to `VoteConversions.VoteMembersDTOOps.toDO` under the
 * hood; that path is already property-tested in shared-models, so we only need to verify the integration at this layer
 * (DO shape + resolver call orchestration).
 */
class HouseVoteConverterSpec extends AnyFlatSpec with Matchers with MockitoSugar {

  private val logCtx = LogContext(runId = "r", stepName = "test")

  // ------------------------------------------------------------------
  // Fixtures
  // ------------------------------------------------------------------

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

  private def mkFixture(
    bioguideToMemberId: Map[String, Long] = Map.empty,
    billLookup: Map[String, Long] = Map.empty,
  ): (HouseVoteConverter[IO], MemberResolver[IO], BillResolver[IO]) = {
    val memberResolver = mock[MemberResolver[IO]]
    when(memberResolver.resolveBatch(any[List[String]], any[LogContext]))
      .thenReturn(IO.pure(bioguideToMemberId))

    val billResolver = mock[BillResolver[IO]]
    billLookup.foreach {
      case (nk, id) =>
        when(billResolver.resolve(eqTo(nk), any[LogContext])).thenReturn(IO.pure(id))
    }

    val converter = new HouseVoteConverter[IO](memberResolver, billResolver, mkLogger)
    (converter, memberResolver, billResolver)
  }

  // ------------------------------------------------------------------
  // Happy paths
  // ------------------------------------------------------------------

  "convert" should "produce VoteDO + VotePositionDO list for a bill-linked vote with one position" in {
    val (converter, memberResolver, billResolver) = mkFixture(
      bioguideToMemberId = Map("A000055" -> 7L),
      billLookup = Map("119-HR-1234" -> 42L),
    )

    val dto = houseDto(positions = List(posDto("A000055", "Yea")))

    val (voteDo, positions) = converter.convert(dto, logCtx).unsafeRunSync()

    val _ = voteDo.naturalKey shouldBe "119-House-1-42"
    val _ = voteDo.billId shouldBe Some(42L)
    val _ = voteDo.chamber shouldBe Chamber.House
    val _ = voteDo.question shouldBe Some("On Passage")
    val _ = positions.length shouldBe 1
    val p = positions.headOption.getOrElse(fail("expected one position"))
    val _ = p.memberId shouldBe Some(7L)
    val _ = p.lisMemberId shouldBe None
    val _ = p.voteId shouldBe 0L // persister rewrites this
    // Resolvers were each called exactly once
    val _ = verify(billResolver, times(1)).resolve(eqTo("119-HR-1234"), any[LogContext])
    verify(memberResolver, times(1)).resolveBatch(any[List[String]], any[LogContext])
  }

  it should "leave VoteDO.billId = None for a procedural vote (no legislationType/number)" in {
    val (converter, memberResolver, billResolver) = mkFixture(
      bioguideToMemberId = Map("A000055" -> 7L)
    )

    val dto = houseDto(
      legislationType = None,
      legislationNumber = None,
      positions = List(posDto("A000055")),
    )

    val (voteDo, _) = converter.convert(dto, logCtx).unsafeRunSync()

    val _ = voteDo.billId shouldBe None
    val _ = verify(billResolver, never()).resolve(anyString(), any[LogContext])
    // Member resolution still runs
    verify(memberResolver, times(1)).resolveBatch(any[List[String]], any[LogContext])
  }

  it should "filter out positions whose bioguide is missing from the resolver's output map" in {
    // Resolver returns only A000055; B000066 is missing — defensive drop in materializePositions
    val (converter, _, _) = mkFixture(
      bioguideToMemberId = Map("A000055" -> 7L),
      billLookup = Map("119-HR-1234" -> 42L),
    )

    val dto = houseDto(positions = List(posDto("A000055"), posDto("B000066")))

    val (_, positions) = converter.convert(dto, logCtx).unsafeRunSync()

    val _ = positions.length shouldBe 1
    positions.headOption.getOrElse(fail("expected one position")).memberId shouldBe Some(7L)
  }

  // ------------------------------------------------------------------
  // Failure paths
  // ------------------------------------------------------------------

  it should "raise VoteConversionFailed when validation returns Left (e.g., invalid congress)" in {
    val (converter, _, _) = mkFixture()

    val dto = houseDto(congress = 0) // congress <= 0 triggers the Left branch

    val outcome = converter.convert(dto, logCtx).attempt.unsafeRunSync()

    outcome match {
      case Left(e: VoteConversionFailed) =>
        e.getMessage should include("Conversion failed for vote")
      case other => fail(s"expected Left(VoteConversionFailed), got $other")
    }
  }

  it should "propagate a MemberResolutionFailed raised by the underlying member resolver" in {
    val memberResolver = mock[MemberResolver[IO]]
    val expected = repcheck.ingestion.votes.errors.MemberResolutionFailed(
      bioguideId = "A000055",
      detail = "fake resolution failure for test",
    )
    when(memberResolver.resolveBatch(any[List[String]], any[LogContext]))
      .thenReturn(IO.raiseError[Map[String, Long]](expected))

    val billResolver = mock[BillResolver[IO]]
    when(billResolver.resolve(eqTo("119-HR-1234"), any[LogContext])).thenReturn(IO.pure(42L))

    val converter = new HouseVoteConverter[IO](memberResolver, billResolver, mkLogger)
    val dto       = houseDto(positions = List(posDto("A000055")))

    val outcome = converter.convert(dto, logCtx).attempt.unsafeRunSync()
    outcome match {
      case Left(e)  => e shouldBe expected
      case Right(_) => fail("expected the resolver's raised failure to propagate")
    }
  }

  // ------------------------------------------------------------------
  // Helpers
  // ------------------------------------------------------------------

  "buildNaturalKey" should "match VoteConversions.buildVoteNaturalKey formatting" in {
    val (converter, _, _) = mkFixture()
    val dto               = houseDto(rollCall = 17, congress = 119, session = 1)
    converter.buildNaturalKey(dto) shouldBe "119-House-1-17"
  }

  it should "use session = 0 when the DTO's sessionNumber is None" in {
    val (converter, _, _) = mkFixture()
    val dto =
      houseDto().copy(sessionNumber = None)
    converter.buildNaturalKey(dto) shouldBe "119-House-0-42"
  }

}
