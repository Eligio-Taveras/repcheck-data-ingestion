package repcheck.ingestion.votes.pipeline

import java.time.{Instant, LocalDate}

import cats.effect.IO
import cats.effect.unsafe.implicits.global

import doobie._
import doobie.free.connection

import org.mockito.ArgumentMatchers.{any, anyLong}
import org.mockito.Mockito.{never, times, verify, when}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar
import repcheck.ingestion.votes.repo.{VoteHistoryArchiver, VotePositionRepository, VoteRepository}
import repcheck.shared.models.congress.common.{BillType, Chamber, Party, UsState}
import repcheck.shared.models.congress.dos.vote.{VoteDO, VotePositionDO}
import repcheck.shared.models.congress.vote.{VoteCast, VoteMethod, VoteType}

/**
 * Unit spec for [[VotePersister]]. The three repositories are mocked; each mocked method returns a `ConnectionIO.pure`
 * value so no actual SQL runs. A minimal in-memory H2 transactor satisfies the `.transact(xa)` boundary — the
 * `ConnectionIO.pure`s compose without touching the connection.
 *
 * Matches the pattern used by `bill-metadata-pipeline/BillPersisterSpec`.
 */
class VotePersisterSpec extends AnyFlatSpec with Matchers with MockitoSugar {

  private val testXa: Transactor[IO] = Transactor.fromDriverManager[IO](
    driver = "org.h2.Driver",
    url = "jdbc:h2:mem:votepersister;DB_CLOSE_DELAY=-1",
    user = "",
    password = "",
    logHandler = None,
  )

  private def baseVoteDO(voteId: Long = 0L): VoteDO =
    VoteDO(
      voteId = voteId,
      naturalKey = "119-House-1-42",
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
      legislationUrl = None,
      sourceDataUrl = None,
      updateDate = Some(Instant.parse("2024-06-01T12:00:00Z")),
      createdAt = None,
      updatedAt = None,
    )

  private def pos(memberId: Long, voteId: Long = 0L): VotePositionDO =
    VotePositionDO(
      id = 0L,
      voteId = voteId,
      memberId = Some(memberId),
      position = Some(VoteCast.Yea),
      partyAtVote = Some(Party.Democrat),
      stateAtVote = Some(UsState.NewYork),
      createdAt = None,
      lisMemberId = None,
    )

  private def mkFixture(): (VotePersister[IO], VoteRepository, VotePositionRepository, VoteHistoryArchiver) = {
    val voteRepo        = mock[VoteRepository]
    val positionRepo    = mock[VotePositionRepository]
    val historyArchiver = mock[VoteHistoryArchiver]
    val persister       = new VotePersister[IO](voteRepo, positionRepo, historyArchiver, testXa)
    (persister, voteRepo, positionRepo, historyArchiver)
  }

  // ------------------------------------------------------------------
  // persistNew
  // ------------------------------------------------------------------

  "persistNew" should "upsert the vote, rewrite position voteIds to the returned id, and replace positions — without archiving" in {
    val (persister, voteRepo, positionRepo, historyArchiver) = mkFixture()

    val incoming  = baseVoteDO(voteId = 0L)
    val persisted = baseVoteDO(voteId = 42L)
    when(voteRepo.upsert(any[VoteDO])).thenReturn(connection.pure(persisted))
    when(positionRepo.replaceAll(anyLong(), any[List[VotePositionDO]])).thenReturn(connection.pure(()))

    val positions = List(pos(1L), pos(2L))

    val result = persister.persistNew(incoming, positions).unsafeRunSync()

    val _ = result shouldBe persisted
    val _ = verify(historyArchiver, never()).archiveVote(anyLong())
    val _ = verify(voteRepo, times(1)).upsert(incoming)

    // Positions should have been rewritten to the upserted vote's voteId (42L), then handed to replaceAll(42L, ...)
    val captor = org.mockito.ArgumentCaptor.forClass(classOf[List[VotePositionDO]])
    val _      = verify(positionRepo, times(1)).replaceAll(org.mockito.ArgumentMatchers.eq(42L), captor.capture())
    val actualPositions = captor.getValue
    all(actualPositions.map(_.voteId)) shouldBe 42L
  }

  // ------------------------------------------------------------------
  // persistUpdate
  // ------------------------------------------------------------------

  "persistUpdate" should "archive the stored vote first, then upsert + replaceAll" in {
    val (persister, voteRepo, positionRepo, historyArchiver) = mkFixture()

    val incoming  = baseVoteDO(voteId = 0L)
    val persisted = baseVoteDO(voteId = 42L)
    when(historyArchiver.archiveVote(anyLong())).thenReturn(connection.pure(999L))
    when(voteRepo.upsert(any[VoteDO])).thenReturn(connection.pure(persisted))
    when(positionRepo.replaceAll(anyLong(), any[List[VotePositionDO]])).thenReturn(connection.pure(()))

    val result = persister.persistUpdate(incoming, List(pos(1L)), storedVoteId = 99L).unsafeRunSync()

    val _ = result shouldBe persisted
    // Archive uses the stored id
    val _ = verify(historyArchiver, times(1)).archiveVote(99L)
    // Upsert runs with the incoming DO
    val _ = verify(voteRepo, times(1)).upsert(incoming)
    // replaceAll runs with the upsert's returned voteId (42L)
    verify(positionRepo, times(1))
      .replaceAll(org.mockito.ArgumentMatchers.eq(42L), any[List[VotePositionDO]])
  }

  // ------------------------------------------------------------------
  // persistMetadataOnlyUpdate
  // ------------------------------------------------------------------

  "persistMetadataOnlyUpdate" should "archive the stored vote and upsert, but NEVER touch the position repository" in {
    val (persister, voteRepo, positionRepo, historyArchiver) = mkFixture()

    val incoming  = baseVoteDO(voteId = 0L)
    val persisted = baseVoteDO(voteId = 42L)
    when(historyArchiver.archiveVote(anyLong())).thenReturn(connection.pure(888L))
    when(voteRepo.upsert(any[VoteDO])).thenReturn(connection.pure(persisted))

    val result = persister.persistMetadataOnlyUpdate(incoming, storedVoteId = 55L).unsafeRunSync()

    val _ = result shouldBe persisted
    val _ = verify(historyArchiver, times(1)).archiveVote(55L)
    val _ = verify(voteRepo, times(1)).upsert(incoming)
    val _ = verify(positionRepo, never()).replaceAll(anyLong(), any[List[VotePositionDO]])
    verify(positionRepo, never()).upsert(any[VotePositionDO])
  }

  // ------------------------------------------------------------------
  // Empty-position list path
  // ------------------------------------------------------------------

  it should "still call replaceAll with Nil when persistNew is given an empty position list (clears the vote's positions)" in {
    val (persister, voteRepo, positionRepo, _) = mkFixture()

    val persisted = baseVoteDO(voteId = 42L)
    when(voteRepo.upsert(any[VoteDO])).thenReturn(connection.pure(persisted))
    when(positionRepo.replaceAll(anyLong(), any[List[VotePositionDO]])).thenReturn(connection.pure(()))

    val _ = persister.persistNew(baseVoteDO(), List.empty).unsafeRunSync()

    verify(positionRepo, times(1))
      .replaceAll(org.mockito.ArgumentMatchers.eq(42L), org.mockito.ArgumentMatchers.eq(List.empty[VotePositionDO]))
  }

}
