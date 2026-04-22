package repcheck.ingestion.votes.repo

import cats.effect.unsafe.implicits.global
import cats.syntax.all._

import doobie.ConnectionIO
import doobie.implicits._

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import repcheck.ingestion.votes.testing.{DockerRequired, TransactorFixture}
import repcheck.shared.models.congress.common.{Party, UsState}
import repcheck.shared.models.congress.dos.vote.VotePositionDO
import repcheck.shared.models.congress.vote.VoteCast

/**
 * Integration tests for [[DoobieVoteHistoryArchiver]] against a real AlloyDB Omni container. Covers §6.3 AC rows 11-15:
 * archiveVote copies the live vote into vote_history, copies positions into vote_history_positions, uses a shared
 * history_id, is a no-op for missing votes, and is composable under a transaction.
 */
class DoobieVoteHistoryArchiverSpec extends AnyFlatSpec with Matchers with TransactorFixture {

  private lazy val archiver     = new DoobieVoteHistoryArchiver
  private lazy val positionRepo = new DoobieVotePositionRepository

  private def seedVoteWithPositions(rollNumber: Int, memberCount: Int, naturalKey: String): Long = {
    val voteId = insertVote(naturalKey, rollNumber = rollNumber)
    val positions = (1 to memberCount).toList.map { i =>
      val memberId = insertMember(f"H$rollNumber-$i%04d")
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
    }
    positionRepo.replaceAll(voteId, positions).transact(xa).unsafeRunSync()
    voteId
  }

  // AC#11
  "archiveVote" should "copy the live votes row into vote_history" taggedAs DockerRequired in {
    val voteId    = insertVote("house:118:1:200", rollNumber = 200)
    val historyId = archiver.archiveVote(voteId).transact(xa).unsafeRunSync()

    val _ = historyId should be > 0L
    val archivedCount =
      sql"SELECT COUNT(*) FROM vote_history WHERE id = $historyId".query[Long].unique.transact(xa).unsafeRunSync()
    val _ = archivedCount shouldBe 1L

    val voteIdOnHistory =
      sql"SELECT vote_id FROM vote_history WHERE id = $historyId".query[Long].unique.transact(xa).unsafeRunSync()
    voteIdOnHistory shouldBe voteId
  }

  // AC#12
  it should "copy every vote_positions row into vote_history_positions" taggedAs DockerRequired in {
    val voteId    = seedVoteWithPositions(rollNumber = 201, memberCount = 100, naturalKey = "house:118:1:201")
    val historyId = archiver.archiveVote(voteId).transact(xa).unsafeRunSync()

    val archivedPositions =
      sql"SELECT COUNT(*) FROM vote_history_positions WHERE history_id = $historyId"
        .query[Long]
        .unique
        .transact(xa)
        .unsafeRunSync()
    archivedPositions shouldBe 100L
  }

  // AC#13
  it should "use the same history_id for the vote row and all its positions" taggedAs DockerRequired in {
    val voteId    = seedVoteWithPositions(rollNumber = 202, memberCount = 5, naturalKey = "house:118:1:202")
    val historyId = archiver.archiveVote(voteId).transact(xa).unsafeRunSync()

    val historyRowPresent =
      sql"SELECT EXISTS(SELECT 1 FROM vote_history WHERE id = $historyId)"
        .query[Boolean]
        .unique
        .transact(xa)
        .unsafeRunSync()
    val _ = historyRowPresent shouldBe true

    val distinctHistoryIds =
      sql"SELECT DISTINCT history_id FROM vote_history_positions WHERE history_id = $historyId"
        .query[Long]
        .to[List]
        .transact(xa)
        .unsafeRunSync()
    distinctHistoryIds shouldBe List(historyId)
  }

  it should "produce a distinct history_id on every subsequent archive of the same vote" taggedAs DockerRequired in {
    val voteId   = seedVoteWithPositions(rollNumber = 203, memberCount = 2, naturalKey = "house:118:1:203")
    val firstId  = archiver.archiveVote(voteId).transact(xa).unsafeRunSync()
    val secondId = archiver.archiveVote(voteId).transact(xa).unsafeRunSync()
    val thirdId  = archiver.archiveVote(voteId).transact(xa).unsafeRunSync()

    List(firstId, secondId, thirdId).distinct.size shouldBe 3
  }

  // AC#14
  it should "be a no-op for a non-existent vote id (no history rows, no error)" taggedAs DockerRequired in {
    val result = archiver.archiveVote(999999L).transact(xa).unsafeRunSync()
    val _      = result shouldBe 0L

    val historyCount =
      sql"SELECT COUNT(*) FROM vote_history WHERE vote_id = 999999".query[Long].unique.transact(xa).unsafeRunSync()
    historyCount shouldBe 0L
  }

  // AC#15
  it should "compose inside a single transaction so a downstream failure rolls back the archive" taggedAs DockerRequired in {
    val voteId = seedVoteWithPositions(rollNumber = 204, memberCount = 3, naturalKey = "house:118:1:204")

    // Compose: archiveVote then deliberately fail (raise inside the ConnectionIO to trigger a rollback).
    val rollback: ConnectionIO[Long] = for {
      historyId <- archiver.archiveVote(voteId)
      _         <- sql"SELECT 1/0".query[Int].unique // triggers a SQL division-by-zero error
    } yield historyId

    val attempt = scala.util.Try(rollback.transact(xa).unsafeRunSync())
    val _       = attempt.isFailure shouldBe true

    val historyCount =
      sql"SELECT COUNT(*) FROM vote_history WHERE vote_id = $voteId".query[Long].unique.transact(xa).unsafeRunSync()
    val positionsCount =
      sql"""SELECT COUNT(*) FROM vote_history_positions
            WHERE history_id IN (SELECT id FROM vote_history WHERE vote_id = $voteId)"""
        .query[Long]
        .unique
        .transact(xa)
        .unsafeRunSync()

    val _ = historyCount shouldBe 0L
    positionsCount shouldBe 0L
  }

  it should "commit the archive when composed with a successful upsert in the same transaction" taggedAs DockerRequired in {
    val voteId    = seedVoteWithPositions(rollNumber = 205, memberCount = 2, naturalKey = "house:118:1:205")
    val combined  = archiver.archiveVote(voteId).flatTap(_ => sql"SELECT 1".query[Int].unique.void)
    val historyId = combined.transact(xa).unsafeRunSync()

    val _ = historyId should be > 0L
    val historyRow =
      sql"SELECT COUNT(*) FROM vote_history WHERE id = $historyId".query[Long].unique.transact(xa).unsafeRunSync()
    historyRow shouldBe 1L
  }

}
