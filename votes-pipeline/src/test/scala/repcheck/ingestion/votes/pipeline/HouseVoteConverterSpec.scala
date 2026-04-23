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
 * Unit spec for [[HouseVoteConverter]]. The converter is pure (aside from logging on the error path and the injected
 * `billLookup` function) — it wraps `VoteConversions.VoteMembersDTOOps.toDO` and returns the resulting
 * [[repcheck.shared.models.congress.dos.results.VoteConversionResult]] directly. Member resolution and `VotePositionDO`
 * materialization happen in the processor.
 */
class HouseVoteConverterSpec extends AnyFlatSpec with Matchers with MockitoSugar {

  private val logCtx = LogContext(runId = "r", stepName = "test")

  /** Stub that returns `Some(42L)` for every key — good enough for assertions about bill-id propagation. */
  private val stubBillLookup: String => IO[Option[Long]] = _ => IO.pure(Some(42L))

  /** Stub that always raises — proves the converter does NOT swallow bill-lookup errors. */
  private val failingBillLookup: String => IO[Option[Long]] = _ =>
    IO.raiseError(new IllegalStateException("simulated placeholder round-trip failure"))

  /** Stub that tracks every invocation so the test can assert when/how often `billLookup` is called. */
  private def recordingBillLookup(
    target: java.util.concurrent.atomic.AtomicReference[List[String]]
  ): String => IO[Option[Long]] =
    nk => IO.delay(target.updateAndGet(_ :+ nk)).as(Some(1L))

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
  // Happy paths — billLookup is invoked; VoteDO.billId carries the resolved id
  // ------------------------------------------------------------------

  "convert" should "thread billLookup into the shared-models conversion so VoteDO.billId is resolved at the converter" in {
    val converter = new HouseVoteConverter[IO](mkLogger)
    val dto       = houseDto(positions = List(posDto("A000055", "Yea")))

    val result = converter.convert(dto, stubBillLookup, logCtx).unsafeRunSync()

    val _ = result.vote.naturalKey shouldBe "119-House-1-42"
    // billId comes from our stubBillLookup; no more processor-level override.
    val _ = result.vote.billId shouldBe Some(42L)
    // billNaturalKey carries the shared-models-derived key so downstream (stance-mark, event) still has the string.
    val _ = result.billNaturalKey shouldBe Some("119-HR-1234")
    // Positions are unresolved (memberSource = Left(bioguide))
    val _     = result.positions.length shouldBe 1
    val first = result.positions.headOption.getOrElse(fail("expected one position"))
    first.memberSource shouldBe Left("A000055")
  }

  it should "leave billId=None AND skip billLookup entirely for procedural votes (no legislation metadata)" in {
    val converter = new HouseVoteConverter[IO](mkLogger)
    val dto       = houseDto(legislationType = None, legislationNumber = None, positions = List(posDto("A000055")))

    val calls = new java.util.concurrent.atomic.AtomicReference[List[String]](List.empty)
    val result = converter
      .convert(dto, recordingBillLookup(calls), logCtx)
      .unsafeRunSync()

    val _ = result.vote.billId shouldBe None
    val _ = result.billNaturalKey shouldBe None
    // billLookup was never called because the DTO carries no legislation metadata.
    calls.get() shouldBe List.empty
  }

  it should "call billLookup exactly once per converted vote (not per position)" in {
    val converter = new HouseVoteConverter[IO](mkLogger)
    val dto = houseDto(positions = List(posDto("A000055"), posDto("B000999", "Nay"), posDto("C001111", "Present")))

    val calls = new java.util.concurrent.atomic.AtomicReference[List[String]](List.empty)
    val _     = converter.convert(dto, recordingBillLookup(calls), logCtx).unsafeRunSync()

    calls.get() shouldBe List("119-HR-1234")
  }

  // ------------------------------------------------------------------
  // Failure paths
  // ------------------------------------------------------------------

  it should "raise VoteConversionFailed when validation returns Left (e.g., invalid congress)" in {
    val converter = new HouseVoteConverter[IO](mkLogger)
    val dto       = houseDto(congress = 0)

    val outcome = converter.convert(dto, stubBillLookup, logCtx).attempt.unsafeRunSync()

    outcome match {
      case Left(e: VoteConversionFailed) =>
        e.getMessage should include("Conversion failed for vote")
      case other => fail(s"expected Left(VoteConversionFailed), got $other")
    }
  }

  it should "log the error at error level when raising VoteConversionFailed" in {
    val loggerMock = mkLogger
    val converter  = new HouseVoteConverter[IO](loggerMock)
    val dto        = houseDto(congress = -1)

    val _ = converter.convert(dto, stubBillLookup, logCtx).attempt.unsafeRunSync()

    import org.mockito.Mockito.{times, verify}
    verify(loggerMock, times(1)).error(any[LogContext], anyString(), any[Option[Throwable]])
  }

  it should "propagate errors raised by billLookup without wrapping" in {
    val converter = new HouseVoteConverter[IO](mkLogger)
    val dto       = houseDto(positions = List(posDto("A000055")))

    val outcome = converter.convert(dto, failingBillLookup, logCtx).attempt.unsafeRunSync()

    outcome match {
      case Left(e: IllegalStateException) =>
        e.getMessage should include("simulated placeholder round-trip failure")
      case other => fail(s"expected Left(IllegalStateException), got $other")
    }
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
