package repcheck.ingestion.votes.pipeline

import java.time.{Instant, LocalDate}
import java.util.UUID

import scala.jdk.CollectionConverters._

import cats.effect.IO
import cats.effect.unsafe.implicits.global

import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.{any, anyLong, anyString}
import org.mockito.Mockito.{never, times, verify, when}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar
import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}
import repcheck.ingestion.votes.persistence.{VotePositionRepository, VoteRepository}
import repcheck.shared.models.congress.common.{BillType, Chamber, Party, UsState}
import repcheck.shared.models.congress.dos.vote.{VoteDO, VotePositionDO}
import repcheck.shared.models.congress.vote.{VoteCast, VoteMethod}

/**
 * Unit spec for [[VoteChangeDetector]]. All dependencies are stubbed via MockitoScala; no infrastructure required.
 *
 * Covers every row of §6.4 acceptance criteria where a mock-driven assertion is meaningful. Order-independence (AC#9)
 * has a spot-check unit test here plus an exhaustive property-based test in [[VotePositionDiffPropSpec]].
 */
class VoteChangeDetectorSpec extends AnyFlatSpec with Matchers with MockitoSugar {

  // ------------------------------------------------------------------
  // Fixture
  // ------------------------------------------------------------------

  private val correlationId: UUID = UUID.fromString("11111111-2222-3333-4444-555555555555")

  final private case class Fixture(
    voteRepo: VoteRepository[IO],
    positionRepo: VotePositionRepository[IO],
    logger: PipelineLogger[IO],
  ) {

    def detector: VoteChangeDetector[IO] =
      new VoteChangeDetector[IO](voteRepo, positionRepo, logger)

  }

  private def fixture(): Fixture = {
    val loggerMock = mock[PipelineLogger[IO]]
    when(loggerMock.info(any[LogContext], anyString())).thenReturn(IO.unit)
    when(loggerMock.warn(any[LogContext], anyString())).thenReturn(IO.unit)
    when(loggerMock.debug(any[LogContext], anyString())).thenReturn(IO.unit)
    when(loggerMock.error(any[LogContext], anyString(), any[Option[Throwable]])).thenReturn(IO.unit)

    Fixture(
      voteRepo = mock[VoteRepository[IO]],
      positionRepo = mock[VotePositionRepository[IO]],
      logger = loggerMock,
    )
  }

  private def makeVote(
    naturalKey: String = "119-house-42",
    updateDate: Option[Instant] = Some(Instant.parse("2024-06-01T12:00:00Z")),
    voteId: Long = 0L,
  ): VoteDO =
    VoteDO(
      voteId = voteId,
      naturalKey = naturalKey,
      congress = 119,
      chamber = Chamber.House,
      rollNumber = 42,
      sessionNumber = Some(1),
      billId = Some(100L),
      question = Some("On Passage"),
      voteType = Some("Passage"),
      voteMethod = Some(VoteMethod.RecordedVote),
      result = Some("Passed"),
      voteDate = Some(LocalDate.parse("2024-05-30")),
      legislationNumber = Some("1234"),
      legislationType = Some(BillType.HR),
      legislationUrl = Some("https://congress.gov/bill/118/hr/1234"),
      sourceDataUrl = Some("https://api.congress.gov/v3/house-vote/119/1/42"),
      updateDate = updateDate,
      createdAt = None,
      updatedAt = None,
    )

  private def pos(
    memberId: Long,
    cast: VoteCast = VoteCast.Yea,
    voteId: Long = 99L,
  ): VotePositionDO =
    VotePositionDO(
      voteId = voteId,
      memberId = memberId,
      position = Some(cast),
      partyAtVote = Some(Party.Democrat),
      stateAtVote = Some(UsState.NewYork),
      createdAt = None,
    )

  // ------------------------------------------------------------------
  // AC#1 — New vote (not in DB) → New; positions never fetched
  // ------------------------------------------------------------------
  "detect" should "return New when no stored vote exists" in {
    val f        = fixture()
    val incoming = makeVote()
    when(f.voteRepo.findByNaturalKey(anyString())).thenReturn(IO.pure(Option.empty[VoteDO]))

    val report = f.detector.detect(incoming, List(pos(1L), pos(2L)), correlationId).unsafeRunSync()

    val _ = report shouldBe VoteChangeReport.New
    verify(f.positionRepo, never()).findByVoteId(anyLong())
  }

  // ------------------------------------------------------------------
  // AC#2 — Incoming updateDate == stored → Unchanged; positions never fetched
  // ------------------------------------------------------------------
  it should "return Unchanged when incoming updateDate equals stored and NOT fetch positions" in {
    val f        = fixture()
    val sameDate = Instant.parse("2024-06-01T12:00:00Z")
    val incoming = makeVote(updateDate = Some(sameDate))
    val stored   = makeVote(voteId = 99L, updateDate = Some(sameDate))
    when(f.voteRepo.findByNaturalKey(anyString())).thenReturn(IO.pure(Some(stored)))

    val report = f.detector.detect(incoming, List(pos(1L)), correlationId).unsafeRunSync()

    val _ = report shouldBe VoteChangeReport.Unchanged
    verify(f.positionRepo, never()).findByVoteId(anyLong())
  }

  // ------------------------------------------------------------------
  // AC#3 — Incoming older than stored → warn + Unchanged; positions never fetched
  // ------------------------------------------------------------------
  it should "return Unchanged and log a warning when incoming updateDate is older than stored" in {
    val f        = fixture()
    val incoming = makeVote(updateDate = Some(Instant.parse("2024-05-01T00:00:00Z")))
    val stored   = makeVote(voteId = 77L, updateDate = Some(Instant.parse("2024-06-01T00:00:00Z")))
    when(f.voteRepo.findByNaturalKey(anyString())).thenReturn(IO.pure(Some(stored)))

    val report = f.detector.detect(incoming, List(pos(1L)), correlationId).unsafeRunSync()

    val _ = report shouldBe VoteChangeReport.Unchanged
    val _ = verify(f.positionRepo, never()).findByVoteId(anyLong())
    verify(f.logger, times(1)).warn(any[LogContext], anyString())
  }

  // ------------------------------------------------------------------
  // AC#5 — Newer updateDate but positions identical → Updated(positionsChanged=false)
  // ------------------------------------------------------------------
  it should "return Updated(positionsChanged=false) when only metadata changed (positions identical)" in {
    val f         = fixture()
    val incoming  = makeVote(updateDate = Some(Instant.parse("2024-07-01T00:00:00Z")))
    val stored    = makeVote(voteId = 55L, updateDate = Some(Instant.parse("2024-06-01T00:00:00Z")))
    val positions = List(pos(1L, VoteCast.Yea), pos(2L, VoteCast.Nay))
    when(f.voteRepo.findByNaturalKey(anyString())).thenReturn(IO.pure(Some(stored)))
    when(f.positionRepo.findByVoteId(anyLong())).thenReturn(IO.pure(positions))

    val report = f.detector.detect(incoming, positions, correlationId).unsafeRunSync()

    report shouldBe VoteChangeReport.Updated(positionsChanged = false, diffs = List.empty)
  }

  // ------------------------------------------------------------------
  // AC#4, AC#6 — Added-only diff (new member not in stored)
  // ------------------------------------------------------------------
  it should "detect an Added diff when incoming has a new memberId absent from stored" in {
    val f                 = fixture()
    val incoming          = makeVote(updateDate = Some(Instant.parse("2024-07-01T00:00:00Z")))
    val stored            = makeVote(voteId = 55L, updateDate = Some(Instant.parse("2024-06-01T00:00:00Z")))
    val storedPositions   = List(pos(1L, VoteCast.Yea))
    val incomingPositions = List(pos(1L, VoteCast.Yea), pos(2L, VoteCast.Nay))
    when(f.voteRepo.findByNaturalKey(anyString())).thenReturn(IO.pure(Some(stored)))
    when(f.positionRepo.findByVoteId(anyLong())).thenReturn(IO.pure(storedPositions))

    val report = f.detector.detect(incoming, incomingPositions, correlationId).unsafeRunSync()

    val _ = report match {
      case VoteChangeReport.Updated(true, diffs) =>
        diffs should contain theSameElementsAs List(VotePositionDiff.Added(2L, "Nay"))
      case other => fail(s"expected Updated(true, _), got $other")
    }
    succeed
  }

  // ------------------------------------------------------------------
  // AC#7 — Removed diff (stored member not in incoming)
  // ------------------------------------------------------------------
  it should "detect a Removed diff when stored has a memberId absent from incoming" in {
    val f                 = fixture()
    val incoming          = makeVote(updateDate = Some(Instant.parse("2024-07-01T00:00:00Z")))
    val stored            = makeVote(voteId = 55L, updateDate = Some(Instant.parse("2024-06-01T00:00:00Z")))
    val storedPositions   = List(pos(1L, VoteCast.Yea), pos(2L, VoteCast.Nay))
    val incomingPositions = List(pos(1L, VoteCast.Yea))
    when(f.voteRepo.findByNaturalKey(anyString())).thenReturn(IO.pure(Some(stored)))
    when(f.positionRepo.findByVoteId(anyLong())).thenReturn(IO.pure(storedPositions))

    val report = f.detector.detect(incoming, incomingPositions, correlationId).unsafeRunSync()

    val _ = report match {
      case VoteChangeReport.Updated(true, diffs) =>
        diffs should contain theSameElementsAs List(VotePositionDiff.Removed(2L, "Nay"))
      case other => fail(s"expected Updated(true, _), got $other")
    }
    succeed
  }

  // ------------------------------------------------------------------
  // AC#8 — Changed diff (same memberId, different cast)
  // ------------------------------------------------------------------
  it should "detect a Changed diff when a member's vote cast changed" in {
    val f                 = fixture()
    val incoming          = makeVote(updateDate = Some(Instant.parse("2024-07-01T00:00:00Z")))
    val stored            = makeVote(voteId = 55L, updateDate = Some(Instant.parse("2024-06-01T00:00:00Z")))
    val storedPositions   = List(pos(1L, VoteCast.Yea))
    val incomingPositions = List(pos(1L, VoteCast.Nay))
    when(f.voteRepo.findByNaturalKey(anyString())).thenReturn(IO.pure(Some(stored)))
    when(f.positionRepo.findByVoteId(anyLong())).thenReturn(IO.pure(storedPositions))

    val report = f.detector.detect(incoming, incomingPositions, correlationId).unsafeRunSync()

    val _ = report match {
      case VoteChangeReport.Updated(true, diffs) =>
        diffs should contain theSameElementsAs List(VotePositionDiff.Changed(1L, "Yea", "Nay"))
      case other => fail(s"expected Updated(true, _), got $other")
    }
    succeed
  }

  // ------------------------------------------------------------------
  // AC#4, AC#6-8 — Mixed: Added + Removed + Changed in the same report
  // ------------------------------------------------------------------
  it should "detect Added + Removed + Changed diffs together in a mixed-change scenario" in {
    val f        = fixture()
    val incoming = makeVote(updateDate = Some(Instant.parse("2024-07-01T00:00:00Z")))
    val stored   = makeVote(voteId = 55L, updateDate = Some(Instant.parse("2024-06-01T00:00:00Z")))
    // Stored: 1=Yea, 2=Yea (removed), 3=Yea (changed to Nay)
    // Incoming: 1=Yea (unchanged), 3=Nay (changed), 4=Present (added)
    val storedPositions = List(
      pos(1L, VoteCast.Yea),
      pos(2L, VoteCast.Yea),
      pos(3L, VoteCast.Yea),
    )
    val incomingPositions = List(
      pos(1L, VoteCast.Yea),
      pos(3L, VoteCast.Nay),
      pos(4L, VoteCast.Present),
    )
    when(f.voteRepo.findByNaturalKey(anyString())).thenReturn(IO.pure(Some(stored)))
    when(f.positionRepo.findByVoteId(anyLong())).thenReturn(IO.pure(storedPositions))

    val report = f.detector.detect(incoming, incomingPositions, correlationId).unsafeRunSync()

    val _ = report match {
      case VoteChangeReport.Updated(true, diffs) =>
        diffs should contain theSameElementsAs List(
          VotePositionDiff.Added(4L, "Present"),
          VotePositionDiff.Removed(2L, "Yea"),
          VotePositionDiff.Changed(3L, "Yea", "Nay"),
        )
      case other => fail(s"expected Updated(true, _), got $other")
    }
    succeed
  }

  // ------------------------------------------------------------------
  // AC#9 — order independence (spot check; exhaustive property test lives in VotePositionDiffPropSpec)
  // ------------------------------------------------------------------
  it should "treat position lists as sets when diffing (order independent — spot check)" in {
    val f        = fixture()
    val incoming = makeVote(updateDate = Some(Instant.parse("2024-07-01T00:00:00Z")))
    val stored   = makeVote(voteId = 55L, updateDate = Some(Instant.parse("2024-06-01T00:00:00Z")))
    val base     = List(pos(1L, VoteCast.Yea), pos(2L, VoteCast.Nay), pos(3L, VoteCast.Present))
    when(f.voteRepo.findByNaturalKey(anyString())).thenReturn(IO.pure(Some(stored)))
    when(f.positionRepo.findByVoteId(anyLong())).thenReturn(IO.pure(base.reverse))

    val report = f.detector.detect(incoming, base, correlationId).unsafeRunSync()

    report shouldBe VoteChangeReport.Updated(positionsChanged = false, diffs = List.empty)
  }

  // ------------------------------------------------------------------
  // AC#10 — Diffs logged at info level (count matches diffs.length)
  // ------------------------------------------------------------------
  it should "log one info entry per diff when the Updated report has diffs" in {
    val f               = fixture()
    val incoming        = makeVote(updateDate = Some(Instant.parse("2024-07-01T00:00:00Z")))
    val stored          = makeVote(voteId = 55L, updateDate = Some(Instant.parse("2024-06-01T00:00:00Z")))
    val storedPositions = List(pos(1L, VoteCast.Yea), pos(2L, VoteCast.Yea), pos(3L, VoteCast.Yea))
    val incomingPositions = List(
      pos(1L, VoteCast.Nay), // changed
      pos(2L, VoteCast.Yea), // unchanged
      // 3 removed
      pos(4L, VoteCast.Present), // added
    )
    when(f.voteRepo.findByNaturalKey(anyString())).thenReturn(IO.pure(Some(stored)))
    when(f.positionRepo.findByVoteId(anyLong())).thenReturn(IO.pure(storedPositions))

    val _ = f.detector.detect(incoming, incomingPositions, correlationId).unsafeRunSync()

    // 1 summary log + 3 per-diff logs = 4 info entries
    verify(f.logger, times(4)).info(any[LogContext], anyString())
  }

  // ------------------------------------------------------------------
  // AC#11 — Correlation ID included in every log context
  // ------------------------------------------------------------------
  it should "include the supplied correlationId in every log context" in {
    val f                 = fixture()
    val incoming          = makeVote(updateDate = Some(Instant.parse("2024-07-01T00:00:00Z")))
    val stored            = makeVote(voteId = 55L, updateDate = Some(Instant.parse("2024-06-01T00:00:00Z")))
    val storedPositions   = List(pos(1L, VoteCast.Yea))
    val incomingPositions = List(pos(1L, VoteCast.Nay))
    when(f.voteRepo.findByNaturalKey(anyString())).thenReturn(IO.pure(Some(stored)))
    when(f.positionRepo.findByVoteId(anyLong())).thenReturn(IO.pure(storedPositions))

    val _ = f.detector.detect(incoming, incomingPositions, correlationId).unsafeRunSync()

    val infoCtx = ArgumentCaptor.forClass(classOf[LogContext])
    val _       = verify(f.logger, org.mockito.Mockito.atLeastOnce()).info(infoCtx.capture(), anyString())
    infoCtx.getAllValues.asScala.toList.foreach(_.correlationId shouldBe Some(correlationId))

    // entityId also present (naturalKey) — useful for downstream log-search by vote key
    infoCtx.getAllValues.asScala.toList.foreach(_.entityId shouldBe Some(incoming.naturalKey))
  }

  // ------------------------------------------------------------------
  // AC#11 variant — warn path also carries correlationId
  // ------------------------------------------------------------------
  it should "include correlationId in the warn context when incoming regresses" in {
    val f        = fixture()
    val incoming = makeVote(updateDate = Some(Instant.parse("2024-05-01T00:00:00Z")))
    val stored   = makeVote(voteId = 77L, updateDate = Some(Instant.parse("2024-06-01T00:00:00Z")))
    when(f.voteRepo.findByNaturalKey(anyString())).thenReturn(IO.pure(Some(stored)))

    val _ = f.detector.detect(incoming, List.empty, correlationId).unsafeRunSync()

    val warnCtx = ArgumentCaptor.forClass(classOf[LogContext])
    val _       = verify(f.logger, times(1)).warn(warnCtx.capture(), anyString())
    warnCtx.getValue.correlationId shouldBe Some(correlationId)
  }

  // ------------------------------------------------------------------
  // AC#12/#13 — Updated(positionsChanged=true) vs Updated(positionsChanged=false) emission gate
  // Verified here by asserting the returned ADT shape; actual event emission lives in VoteProcessor (P3.1).
  // ------------------------------------------------------------------
  it should "set positionsChanged=true when at least one diff is produced" in {
    val f        = fixture()
    val incoming = makeVote(updateDate = Some(Instant.parse("2024-07-01T00:00:00Z")))
    val stored   = makeVote(voteId = 55L, updateDate = Some(Instant.parse("2024-06-01T00:00:00Z")))
    when(f.voteRepo.findByNaturalKey(anyString())).thenReturn(IO.pure(Some(stored)))
    when(f.positionRepo.findByVoteId(anyLong())).thenReturn(IO.pure(List(pos(1L, VoteCast.Yea))))

    val report =
      f.detector.detect(incoming, List(pos(1L, VoteCast.Nay)), correlationId).unsafeRunSync()

    report match {
      case VoteChangeReport.Updated(true, diffs) => diffs should not be empty
      case other                                 => fail(s"expected Updated(true, _), got $other")
    }
  }

  it should "set positionsChanged=false when no diffs are produced" in {
    val f         = fixture()
    val incoming  = makeVote(updateDate = Some(Instant.parse("2024-07-01T00:00:00Z")))
    val stored    = makeVote(voteId = 55L, updateDate = Some(Instant.parse("2024-06-01T00:00:00Z")))
    val positions = List(pos(1L, VoteCast.Yea), pos(2L, VoteCast.Nay))
    when(f.voteRepo.findByNaturalKey(anyString())).thenReturn(IO.pure(Some(stored)))
    when(f.positionRepo.findByVoteId(anyLong())).thenReturn(IO.pure(positions))

    val report = f.detector.detect(incoming, positions, correlationId).unsafeRunSync()

    report match {
      case VoteChangeReport.Updated(false, diffs) => diffs shouldBe empty
      case other                                  => fail(s"expected Updated(false, _), got $other")
    }
  }

  // ------------------------------------------------------------------
  // Edge case: stored voteId is threaded into positionRepo.findByVoteId
  // ------------------------------------------------------------------
  it should "fetch stored positions using the stored vote's voteId (not the incoming's 0L)" in {
    val f        = fixture()
    val incoming = makeVote(updateDate = Some(Instant.parse("2024-07-01T00:00:00Z")))
    val stored   = makeVote(voteId = 777L, updateDate = Some(Instant.parse("2024-06-01T00:00:00Z")))
    when(f.voteRepo.findByNaturalKey(anyString())).thenReturn(IO.pure(Some(stored)))
    when(f.positionRepo.findByVoteId(anyLong())).thenReturn(IO.pure(List.empty[VotePositionDO]))

    val _ = f.detector.detect(incoming, List.empty, correlationId).unsafeRunSync()

    verify(f.positionRepo, times(1)).findByVoteId(777L)
  }

  // ------------------------------------------------------------------
  // Edge case: missing updateDate on either side is treated as Unchanged (never overwrite when we can't date-stamp)
  // ------------------------------------------------------------------
  it should "return Unchanged when incoming updateDate is None" in {
    val f        = fixture()
    val incoming = makeVote(updateDate = None)
    val stored   = makeVote(voteId = 55L, updateDate = Some(Instant.parse("2024-06-01T00:00:00Z")))
    when(f.voteRepo.findByNaturalKey(anyString())).thenReturn(IO.pure(Some(stored)))

    val report = f.detector.detect(incoming, List(pos(1L)), correlationId).unsafeRunSync()

    val _ = report shouldBe VoteChangeReport.Unchanged
    verify(f.positionRepo, never()).findByVoteId(anyLong())
  }

  it should "return Unchanged when stored updateDate is None (defensive)" in {
    val f        = fixture()
    val incoming = makeVote(updateDate = Some(Instant.parse("2024-07-01T00:00:00Z")))
    val stored   = makeVote(voteId = 55L, updateDate = None)
    when(f.voteRepo.findByNaturalKey(anyString())).thenReturn(IO.pure(Some(stored)))

    val report = f.detector.detect(incoming, List(pos(1L)), correlationId).unsafeRunSync()

    val _ = report shouldBe VoteChangeReport.Unchanged
    verify(f.positionRepo, never()).findByVoteId(anyLong())
  }

  // ------------------------------------------------------------------
  // renderDiff sanity — keeps human-readable format in log output
  // ------------------------------------------------------------------
  "renderDiff" should "format Added with memberId + cast" in {
    val f = fixture()
    f.detector.renderDiff("119-house-42", VotePositionDiff.Added(42L, "Yea")) shouldBe
      "Vote 119-house-42: added position memberId=42 cast=Yea"
  }

  it should "format Removed with memberId + previousCast" in {
    val f = fixture()
    f.detector.renderDiff("119-house-42", VotePositionDiff.Removed(42L, "Nay")) shouldBe
      "Vote 119-house-42: removed position memberId=42 previousCast=Nay"
  }

  it should "format Changed with from + to casts" in {
    val f = fixture()
    f.detector.renderDiff("119-house-42", VotePositionDiff.Changed(42L, "Yea", "Nay")) shouldBe
      "Vote 119-house-42: changed position memberId=42 from=Yea to=Nay"
  }

  // ------------------------------------------------------------------
  // metadata-only update still emits the summary info log (single line, no per-diff lines)
  // ------------------------------------------------------------------
  "logDiffs" should "log exactly one info entry for a metadata-only update (no per-diff lines)" in {
    val f         = fixture()
    val incoming  = makeVote(updateDate = Some(Instant.parse("2024-07-01T00:00:00Z")))
    val stored    = makeVote(voteId = 55L, updateDate = Some(Instant.parse("2024-06-01T00:00:00Z")))
    val positions = List(pos(1L, VoteCast.Yea))
    when(f.voteRepo.findByNaturalKey(anyString())).thenReturn(IO.pure(Some(stored)))
    when(f.positionRepo.findByVoteId(anyLong())).thenReturn(IO.pure(positions))

    val _ = f.detector.detect(incoming, positions, correlationId).unsafeRunSync()

    verify(f.logger, times(1)).info(any[LogContext], anyString())
  }

  // ------------------------------------------------------------------
  // castLabel — None maps to "<none>" sentinel
  // ------------------------------------------------------------------
  "VotePositionDiff.castLabel" should "render None as the <none> sentinel" in {
    VotePositionDiff.castLabel(None) shouldBe "<none>"
  }

  it should "render Some(cast) as the enum apiValue" in {
    val _ = VotePositionDiff.castLabel(Some(VoteCast.Yea)) shouldBe "Yea"
    val _ = VotePositionDiff.castLabel(Some(VoteCast.Nay)) shouldBe "Nay"
    val _ = VotePositionDiff.castLabel(Some(VoteCast.Present)) shouldBe "Present"
    val _ = VotePositionDiff.castLabel(Some(VoteCast.NotVoting)) shouldBe "Not Voting"
    VotePositionDiff.castLabel(Some(VoteCast.Absent)) shouldBe "Absent"
  }

  // ------------------------------------------------------------------
  // position.position = None (unknown cast) compares as equal to another None on same member; differs vs Some
  // ------------------------------------------------------------------
  it should "treat a None position as different from Some(cast) when the same memberId appears on both sides" in {
    val f                 = fixture()
    val incoming          = makeVote(updateDate = Some(Instant.parse("2024-07-01T00:00:00Z")))
    val stored            = makeVote(voteId = 55L, updateDate = Some(Instant.parse("2024-06-01T00:00:00Z")))
    val storedPositions   = List(pos(1L, VoteCast.Yea))
    val incomingPositions = List(pos(1L).copy(position = None))
    when(f.voteRepo.findByNaturalKey(anyString())).thenReturn(IO.pure(Some(stored)))
    when(f.positionRepo.findByVoteId(anyLong())).thenReturn(IO.pure(storedPositions))

    val report = f.detector.detect(incoming, incomingPositions, correlationId).unsafeRunSync()

    report match {
      case VoteChangeReport.Updated(true, diffs) =>
        diffs should contain theSameElementsAs List(VotePositionDiff.Changed(1L, "Yea", "<none>"))
      case other => fail(s"expected Updated(true, _), got $other")
    }
  }

}
