package repcheck.ingestion.votes.pipeline

import java.time.{Instant, LocalDate}
import java.util.UUID

import cats.effect.IO
import cats.effect.unsafe.implicits.global

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
import repcheck.shared.models.congress.vote.{VoteCast, VoteMethod}

/**
 * ScalaCheck property tests for [[VoteChangeDetector]]'s position diffing.
 *
 * These cover the "order independence" and "diff completeness" properties called out in the plan's P2.5 section:
 *
 *   - **Order independence (§6.4 AC#9)**: shuffling either the incoming or stored position list yields the same
 *     [[VoteChangeReport]] — per the detector's contract that position lists are treated as sets keyed by `memberId`.
 *   - **Diff completeness**: applying the diffs to the stored position set produces the incoming position set. Proves
 *     the detector isn't under-reporting (missing an added/removed/changed member) or over-reporting (inventing diffs).
 *
 * The detector is exercised via a live [[VoteChangeDetector]] with MockitoScala-stubbed repos so we cover the full
 * `detect` call path, not just the private `computeDiffs` helper. Correlation ID is arbitrary; the logger is a no-op
 * stub.
 */
class VotePositionDiffPropSpec extends AnyFlatSpec with Matchers with MockitoSugar with ScalaCheckPropertyChecks {

  // ------------------------------------------------------------------
  // Tuning — keep generators small but exercise edge cases (empty, single-element, duplicates-on-same-member)
  // ------------------------------------------------------------------
  implicit override val generatorDrivenConfig: PropertyCheckConfiguration =
    PropertyCheckConfiguration(minSuccessful = 100, sizeRange = 20)

  // ------------------------------------------------------------------
  // Generators
  // ------------------------------------------------------------------

  private val voteCastGen: Gen[VoteCast] =
    Gen.oneOf(VoteCast.values.toIndexedSeq)

  /**
   * Generate unique memberIds per list — duplicate memberIds on the same side would collide under `groupMapReduce`, and
   * the processor never feeds duplicates (vote_positions PK is (vote_id, member_id)). We bake that invariant into the
   * generator so the property captures realistic inputs.
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
            voteId = voteId,
            memberId = id,
            position = Some(cast),
            partyAtVote = Some(Party.Democrat),
            stateAtVote = Some(UsState.NewYork),
            createdAt = None,
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
   * Compare two reports by diff-content equality — `Updated` is compared by `positionsChanged` + set-equality of
   * `diffs`, since the diff-list ordering is an implementation detail.
   */
  private def sameReport(a: VoteChangeReport, b: VoteChangeReport): Boolean =
    (a, b) match {
      case (VoteChangeReport.New, VoteChangeReport.New)             => true
      case (VoteChangeReport.Unchanged, VoteChangeReport.Unchanged) => true
      case (VoteChangeReport.Updated(ac, ad), VoteChangeReport.Updated(bc, bd)) =>
        ac == bc && ad.toSet == bd.toSet
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
  // Property: diff completeness — applying diffs to stored-set yields the incoming-set
  // ------------------------------------------------------------------
  it should "produce diffs such that (stored ∆ diffs) == incoming, keyed by memberId + cast" in {
    val stored   = makeVote(voteId = 55L, updateDate = Some(Instant.parse("2024-06-01T00:00:00Z")))
    val incoming = makeVote(updateDate = Some(Instant.parse("2024-07-01T00:00:00Z")))

    forAll { (storedPositions: List[VotePositionDO], incomingPositions: List[VotePositionDO]) =>
      val detector = makeDetector(Some(stored), storedPositions)
      val report   = detector.detect(incoming, incomingPositions, correlationId).unsafeRunSync()

      val diffs = report match {
        case VoteChangeReport.Updated(_, d) => d
        case _                              => List.empty[VotePositionDiff]
      }

      val storedKV: Map[Long, String] =
        storedPositions.map(p => p.memberId -> VotePositionDiff.castLabel(p.position)).toMap
      val expectedIncomingKV: Map[Long, String] =
        incomingPositions.map(p => p.memberId -> VotePositionDiff.castLabel(p.position)).toMap

      val afterApplying: Map[Long, String] = diffs.foldLeft(storedKV) {
        case (acc, VotePositionDiff.Added(id, cast))        => acc + (id -> cast)
        case (acc, VotePositionDiff.Removed(id, _))         => acc - id
        case (acc, VotePositionDiff.Changed(id, _, toCast)) => acc + (id -> toCast)
      }

      afterApplying shouldBe expectedIncomingKV
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

      report shouldBe VoteChangeReport.Updated(positionsChanged = false, diffs = List.empty)
    }
  }

  // ------------------------------------------------------------------
  // Property: diff counts match expected set-theoretic sizes
  // ------------------------------------------------------------------
  it should "emit exactly (|incoming \\ stored|) Added diffs, (|stored \\ incoming|) Removed diffs, " +
    "and at most |incoming ∩ stored| Changed diffs" in {
      val stored   = makeVote(voteId = 55L, updateDate = Some(Instant.parse("2024-06-01T00:00:00Z")))
      val incoming = makeVote(updateDate = Some(Instant.parse("2024-07-01T00:00:00Z")))

      forAll { (storedPositions: List[VotePositionDO], incomingPositions: List[VotePositionDO]) =>
        val detector = makeDetector(Some(stored), storedPositions)
        val report   = detector.detect(incoming, incomingPositions, correlationId).unsafeRunSync()

        val diffs = report match {
          case VoteChangeReport.Updated(_, d) => d
          case _                              => List.empty[VotePositionDiff]
        }

        val storedIds   = storedPositions.map(_.memberId).toSet
        val incomingIds = incomingPositions.map(_.memberId).toSet

        val addedCount = diffs.count {
          case _: VotePositionDiff.Added => true
          case _                         => false
        }
        val removedCount = diffs.count {
          case _: VotePositionDiff.Removed => true
          case _                           => false
        }
        val changedCount = diffs.count {
          case _: VotePositionDiff.Changed => true
          case _                           => false
        }

        val _ = addedCount shouldBe (incomingIds -- storedIds).size
        val _ = removedCount shouldBe (storedIds -- incomingIds).size
        changedCount should be <= (storedIds intersect incomingIds).size
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

}
