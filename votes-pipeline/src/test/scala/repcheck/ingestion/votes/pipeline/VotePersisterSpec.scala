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
 * Unit spec for [[VotePersister]]. The three repositories are mocked; the caller supplies a `positionsFactory: Long =>
 * List[VotePositionDO]` that the persister invokes INSIDE the transaction once the upsert returns the real `voteId`. No
 * `VotePositionDO` with a fake `voteId = 0L` is carried through the pipeline.
 *
 * An in-memory H2 transactor satisfies the `.transact(xa)` boundary — the mocked `ConnectionIO.pure` values compose
 * without touching the connection.
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

  /** Factory closure that materializes positions with a given voteId — exactly how the processor passes it in. */
  private def factory(members: List[Long]): Long => List[VotePositionDO] =
    voteId =>
      members.map(memberId =>
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

  "persistNew" should "upsert the vote, then invoke positionsFactory with the upserted voteId, then replaceAll" in {
    val (persister, voteRepo, positionRepo, historyArchiver) = mkFixture()

    val incoming  = baseVoteDO(voteId = 0L)
    val persisted = baseVoteDO(voteId = 42L)
    when(voteRepo.upsert(any[VoteDO])).thenReturn(connection.pure(persisted))
    when(positionRepo.replaceAll(anyLong(), any[List[VotePositionDO]])).thenReturn(connection.pure(()))

    val positionsFactory = factory(List(1L, 2L))

    val result = persister.persistNew(incoming, positionsFactory).unsafeRunSync()

    val _ = result shouldBe persisted
    val _ = verify(historyArchiver, never()).archiveVote(anyLong())
    val _ = verify(voteRepo, times(1)).upsert(incoming)

    // replaceAll MUST be called with voteId=42L and positions built by the factory using that same id.
    val captor = org.mockito.ArgumentCaptor.forClass(classOf[List[VotePositionDO]])
    val _      = verify(positionRepo, times(1)).replaceAll(org.mockito.ArgumentMatchers.eq(42L), captor.capture())
    val actual = captor.getValue
    val _      = actual.length shouldBe 2
    all(actual.map(_.voteId)) shouldBe 42L
  }

  it should "invoke the factory ONCE with the persisted voteId (no reuse with 0L or any other value)" in {
    val (persister, voteRepo, positionRepo, _) = mkFixture()

    val callsRef = new java.util.concurrent.atomic.AtomicReference[List[Long]](List.empty)
    val spyFactory: Long => List[VotePositionDO] = voteId => {
      val _ = callsRef.updateAndGet(_ :+ voteId)
      List.empty
    }
    when(voteRepo.upsert(any[VoteDO])).thenReturn(connection.pure(baseVoteDO(voteId = 777L)))
    when(positionRepo.replaceAll(anyLong(), any[List[VotePositionDO]])).thenReturn(connection.pure(()))

    val _ = persister.persistNew(baseVoteDO(), spyFactory).unsafeRunSync()

    callsRef.get() shouldBe List(777L)
  }

  // ------------------------------------------------------------------
  // persistUpdate
  // ------------------------------------------------------------------

  "persistUpdate" should "archive the stored voteId first, then upsert, then invoke the factory with the new voteId" in {
    val (persister, voteRepo, positionRepo, historyArchiver) = mkFixture()

    val incoming  = baseVoteDO(voteId = 0L)
    val persisted = baseVoteDO(voteId = 42L)
    when(historyArchiver.archiveVote(anyLong())).thenReturn(connection.pure(999L))
    when(voteRepo.upsert(any[VoteDO])).thenReturn(connection.pure(persisted))
    when(positionRepo.replaceAll(anyLong(), any[List[VotePositionDO]])).thenReturn(connection.pure(()))

    val result = persister.persistUpdate(incoming, storedVoteId = 99L, factory(List(1L))).unsafeRunSync()

    val _ = result shouldBe persisted
    // Archive uses the stored id (99L), not the upserted id (42L)
    val _ = verify(historyArchiver, times(1)).archiveVote(99L)
    val _ = verify(voteRepo, times(1)).upsert(incoming)
    // replaceAll uses the upserted id (42L)
    verify(positionRepo, times(1))
      .replaceAll(org.mockito.ArgumentMatchers.eq(42L), any[List[VotePositionDO]])
  }

  // ------------------------------------------------------------------
  // persistMetadataOnlyUpdate
  // ------------------------------------------------------------------

  "persistMetadataOnlyUpdate" should "archive and upsert, but NEVER touch the position repository" in {
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
  // Empty-factory-result edge case
  // ------------------------------------------------------------------

  it should "still call replaceAll with Nil when the factory yields an empty position list" in {
    val (persister, voteRepo, positionRepo, _) = mkFixture()

    when(voteRepo.upsert(any[VoteDO])).thenReturn(connection.pure(baseVoteDO(voteId = 42L)))
    when(positionRepo.replaceAll(anyLong(), any[List[VotePositionDO]])).thenReturn(connection.pure(()))

    val _ = persister.persistNew(baseVoteDO(), _ => List.empty).unsafeRunSync()

    verify(positionRepo, times(1))
      .replaceAll(org.mockito.ArgumentMatchers.eq(42L), org.mockito.ArgumentMatchers.eq(List.empty[VotePositionDO]))
  }

}
