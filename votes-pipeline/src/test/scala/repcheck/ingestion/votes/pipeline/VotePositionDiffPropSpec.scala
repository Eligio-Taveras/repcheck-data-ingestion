package repcheck.ingestion.votes.pipeline

import java.time.{Instant, LocalDate}
import java.util.UUID

import cats.effect.IO
import cats.effect.unsafe.implicits.global

import difflicious.{DiffResult, Differ}
import org.mockito.ArgumentMatchers.{any, anyLong, anyString}
import org.mockito.Mockito.when
import org.scalacheck.{Arbitrary, Gen}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}
import repcheck.ingestion.votes.persistence.{VotePositionRepository, VoteRepository}
import repcheck.shared.models.congress.common.{BillType, Chamber, Party, UsState}
import repcheck.shared.models.congress.dos.vote.{VoteDO, VotePositionDO}
import repcheck.shared.models.congress.vote.{VoteCast, VoteMethod, VoteType}

/**
 * ScalaCheck property tests for [[VoteChangeDetector]] and its underlying `Differ[List[VotePositionDO]]` from
 * [[VotePositionDiffer]].
 *
 * These cover the properties called out in the §6.4 spec that are hard to exhaustively hit with hand-crafted unit
 * tests:
 *
 *   - **Order independence (§6.4 AC#9)**: shuffling either the incoming or stored position list yields the same
 *     [[VoteChangeReport]] — per the detector's contract that position lists are treated as sets keyed by `memberId`,
 *     enforced by `.pairBy(_.memberId)` inside `VotePositionDiffer`.
 *   - **`isOk` consistency**: `diff.isOk` is `true` iff stored and incoming position sets are identical by `memberId +
 *     VotePositionDO` equality. Proves we don't over-report or under-report.
 *   - **New-vote short-circuit**: when `storedVote = None`, every incoming produces [[VoteChangeReport.New]] without
 *     consulting positions.
 *
 * The detector is exercised via a live [[VoteChangeDetector]] with MockitoScala-stubbed repos so we cover the full
 * `detect` call path, not just the `Differ`. Correlation ID is arbitrary; the logger is a no-op stub.
 */
class VotePositionDiffPropSpec extends AnyFlatSpec with Matchers with MockitoSugar with ScalaCheckPropertyChecks {

  // ------------------------------------------------------------------
  // Tuning — keep generators small but exercise edge cases (empty, single-element)
  // ------------------------------------------------------------------
  implicit override val generatorDrivenConfig: PropertyCheckConfiguration =
    PropertyCheckConfiguration(minSuccessful = 100, sizeRange = 20)

  // ------------------------------------------------------------------
  // Generators
  // ------------------------------------------------------------------

  private val voteCastGen: Gen[VoteCast] =
    Gen.oneOf(VoteCast.values.toIndexedSeq)

  /**
   * Generate unique memberIds per list — duplicate memberIds on the same side would collide inside the differ's
   * pair-matching. The processor never feeds duplicates (vote_positions PK is (vote_id, member_id)), so we bake that
   * invariant into the generator.
   */
  private def positionListGen(voteId: Long): Gen[List[VotePositionDO]] =
    for {
      size <- Gen.choose(0, 15)
      ids  <- Gen.pick(size, 1L to 20L).map(_.toList.distinct)
      positions <- Gen.sequence[List[VotePositionDO], VotePositionDO](
        ids.map(id =>
          for {
            cast <- voteCastGen
          } yield VotePositionDO(
            id = 0L,
            voteId = voteId,
            memberId = Some(id),
            position = Some(cast),
            partyAtVote = Some(Party.Democrat),
            stateAtVote = Some(UsState.NewYork),
            createdAt = None,
            lisMemberId = None,
          )
        )
      )
    } yield positions

  implicit private val arbPositions: Arbitrary[List[VotePositionDO]] =
    Arbitrary(positionListGen(voteId = 99L))

  private def makeVote(
    naturalKey: String = "119-house-42",
    updateDate: Option[Instant],
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
      voteType = Some(VoteType.Passage),
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

  private def makeDetector(
    storedVote: Option[VoteDO],
    storedPositions: List[VotePositionDO],
  ): VoteChangeDetector[IO] = {
    val voteRepo     = mock[VoteRepository[IO]]
    val positionRepo = mock[VotePositionRepository[IO]]
    val loggerMock   = mock[PipelineLogger[IO]]
    when(loggerMock.info(any[LogContext], anyString())).thenReturn(IO.unit)
    when(loggerMock.warn(any[LogContext], anyString())).thenReturn(IO.unit)
    when(loggerMock.debug(any[LogContext], anyString())).thenReturn(IO.unit)
    when(loggerMock.error(any[LogContext], anyString(), any[Option[Throwable]])).thenReturn(IO.unit)
    when(voteRepo.findByNaturalKey(anyString())).thenReturn(IO.pure(storedVote))
    when(positionRepo.findByVoteId(anyLong())).thenReturn(IO.pure(storedPositions))

    new VoteChangeDetector[IO](voteRepo, positionRepo, loggerMock)
  }

  private val correlationId: UUID = UUID.fromString("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")

  /**
   * Compare two reports for equivalence: `Updated` is equivalent iff `positionsChanged` agrees and the underlying diff
   * has the same `isOk` — since the diff-list ordering is a difflicious implementation detail that we want to treat as
   * unobservable from a contract point of view.
   */
  private def sameReport(a: VoteChangeReport, b: VoteChangeReport): Boolean =
    (a, b) match {
      case (VoteChangeReport.New, VoteChangeReport.New)             => true
      case (VoteChangeReport.Unchanged, VoteChangeReport.Unchanged) => true
      case (VoteChangeReport.Updated(ac, ad), VoteChangeReport.Updated(bc, bd)) =>
        ac == bc && ad.isOk == bd.isOk
      case _ => false
    }

  // ------------------------------------------------------------------
  // Property: order independence — shuffling either side yields the same report
  // ------------------------------------------------------------------
  "detect" should
    "produce the same VoteChangeReport regardless of incoming/stored position ordering (set semantics)" in {
      val stored = makeVote(voteId = 55L, updateDate = Some(Instant.parse("2024-06-01T00:00:00Z")))

      forAll { (storedPositions: List[VotePositionDO], incomingPositions: List[VotePositionDO]) =>
        val incoming = makeVote(updateDate = Some(Instant.parse("2024-07-01T00:00:00Z")))
        val detector = makeDetector(Some(stored), storedPositions)

        val baseReport = detector.detect(incoming, incomingPositions, correlationId).unsafeRunSync()

        val shuffledIncoming = scala.util.Random.shuffle(incomingPositions)
        val shuffledReport =
          detector.detect(incoming, shuffledIncoming, correlationId).unsafeRunSync()

        val _ = sameReport(baseReport, shuffledReport) shouldBe true

        // Also flip the stored side for completeness — requires a fresh detector with a shuffled stub
        val detectorWithShuffledStored =
          makeDetector(Some(stored), scala.util.Random.shuffle(storedPositions))
        val shuffledStoredReport =
          detectorWithShuffledStored.detect(incoming, incomingPositions, correlationId).unsafeRunSync()

        sameReport(baseReport, shuffledStoredReport) shouldBe true
      }
    }

  // ------------------------------------------------------------------
  // Property: diff.isOk iff the two position sets are identical by (memberId -> VotePositionDO) equality
  // ------------------------------------------------------------------
  it should "produce diff.isOk == (incomingSet == storedSet) keyed by memberId" in {
    val stored   = makeVote(voteId = 55L, updateDate = Some(Instant.parse("2024-06-01T00:00:00Z")))
    val incoming = makeVote(updateDate = Some(Instant.parse("2024-07-01T00:00:00Z")))

    forAll { (storedPositions: List[VotePositionDO], incomingPositions: List[VotePositionDO]) =>
      val detector = makeDetector(Some(stored), storedPositions)
      val report   = detector.detect(incoming, incomingPositions, correlationId).unsafeRunSync()

      val storedMap    = storedPositions.map(p => p.memberId -> p).toMap
      val incomingMap  = incomingPositions.map(p => p.memberId -> p).toMap
      val expectedIsOk = storedMap == incomingMap

      report match {
        case VoteChangeReport.Updated(positionsChanged, diff) =>
          val _ = diff.isOk shouldBe expectedIsOk
          positionsChanged shouldBe !expectedIsOk
        case other => fail(s"expected Updated(_, _), got $other")
      }
    }
  }

  // ------------------------------------------------------------------
  // Property: identical lists produce Updated(positionsChanged=false) (with newer updateDate)
  // ------------------------------------------------------------------
  it should "produce Updated(positionsChanged=false) when incoming and stored position sets are identical" in {
    val stored   = makeVote(voteId = 55L, updateDate = Some(Instant.parse("2024-06-01T00:00:00Z")))
    val incoming = makeVote(updateDate = Some(Instant.parse("2024-07-01T00:00:00Z")))

    forAll { (positions: List[VotePositionDO]) =>
      val detector = makeDetector(Some(stored), positions)

      // Shuffle the incoming side to prove set semantics — same members/casts, different order.
      val shuffled = scala.util.Random.shuffle(positions)
      val report   = detector.detect(incoming, shuffled, correlationId).unsafeRunSync()

      report match {
        case VoteChangeReport.Updated(false, diff) => diff.isOk shouldBe true
        case other                                 => fail(s"expected Updated(false, _), got $other")
      }
    }
  }

  // ------------------------------------------------------------------
  // Property: detector never raises; every input produces exactly one VoteChangeReport
  // ------------------------------------------------------------------
  it should "always succeed (no raised error) for any combination of incoming/stored position lists" in {
    val stored   = makeVote(voteId = 55L, updateDate = Some(Instant.parse("2024-06-01T00:00:00Z")))
    val incoming = makeVote(updateDate = Some(Instant.parse("2024-07-01T00:00:00Z")))

    forAll { (storedPositions: List[VotePositionDO], incomingPositions: List[VotePositionDO]) =>
      val detector = makeDetector(Some(stored), storedPositions)
      val outcome  = detector.detect(incoming, incomingPositions, correlationId).attempt.unsafeRunSync()
      outcome.isRight shouldBe true
    }
  }

  // ------------------------------------------------------------------
  // Property: the "new vote" branch is always hit when storedVote is None (positions never consulted)
  // ------------------------------------------------------------------
  it should "return New for any incoming payload when storedVote is None" in {
    val incoming = makeVote(updateDate = Some(Instant.parse("2024-07-01T00:00:00Z")))

    forAll { (incomingPositions: List[VotePositionDO]) =>
      val detector = makeDetector(storedVote = None, storedPositions = List.empty)
      val report   = detector.detect(incoming, incomingPositions, correlationId).unsafeRunSync()
      report shouldBe VoteChangeReport.New
    }
  }

  // ------------------------------------------------------------------
  // Property: the underlying `Differ[List[VotePositionDO]]` itself is pair-matched by memberId
  // ------------------------------------------------------------------
  "VotePositionDiffer.votePositionsDiffer" should "match elements by memberId regardless of order" in {
    val differ: Differ[List[VotePositionDO]] = VotePositionDiffer.votePositionsDiffer

    forAll { (positions: List[VotePositionDO]) =>
      val shuffled = scala.util.Random.shuffle(positions)
      val result   = differ.diff(positions, shuffled)
      result.isOk shouldBe true
    }
  }

  it should "produce isOk=false when at least one memberId's cast differs" in {
    val differ: Differ[List[VotePositionDO]] = VotePositionDiffer.votePositionsDiffer

    forAll { (positions: List[VotePositionDO]) =>
      whenever(positions.nonEmpty) {
        val mutated = positions match {
          case first :: rest =>
            val newCast = first.position
              .fold(Some(VoteCast.Yea))(cast => Some(if (cast == VoteCast.Yea) VoteCast.Nay else VoteCast.Yea))
            first.copy(position = newCast) :: rest
          case Nil => Nil
        }
        val result = differ.diff(mutated, positions)
        val _      = result.isOk shouldBe false
        result shouldBe a[DiffResult.ListResult]
      }
    }
  }

  // ------------------------------------------------------------------
  // identityKey — exercise every branch (House, Senate, XOR violation)
  // ------------------------------------------------------------------
  private def pos(memberId: Option[Long], lisMemberId: Option[Long]): VotePositionDO =
    VotePositionDO(
      id = 0L,
      voteId = 42L,
      memberId = memberId,
      position = Some(VoteCast.Yea),
      partyAtVote = Some(Party.Democrat),
      stateAtVote = Some(UsState.NewYork),
      createdAt = None,
      lisMemberId = lisMemberId,
    )

  "VotePositionDiffer.identityKey" should "tag a House row as (\"M\", memberId)" in {
    VotePositionDiffer.identityKey(pos(memberId = Some(7L), lisMemberId = None)) shouldBe (("M", 7L))
  }

  it should "tag a Senate row as (\"L\", lisMemberId)" in {
    VotePositionDiffer.identityKey(pos(memberId = None, lisMemberId = Some(99L))) shouldBe (("L", 99L))
  }

  it should "degenerate to (\"?\", 0L) when both identities are None (XOR violation — defensive)" in {
    VotePositionDiffer.identityKey(pos(memberId = None, lisMemberId = None)) shouldBe (("?", 0L))
  }

  it should "degenerate to (\"?\", 0L) when both identities are Some (XOR violation — defensive)" in {
    VotePositionDiffer.identityKey(pos(memberId = Some(1L), lisMemberId = Some(2L))) shouldBe (("?", 0L))
  }

}
