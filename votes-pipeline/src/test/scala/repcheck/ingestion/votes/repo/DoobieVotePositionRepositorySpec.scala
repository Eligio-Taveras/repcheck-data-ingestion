package repcheck.ingestion.votes.repo

import cats.effect.unsafe.implicits.global

import doobie.implicits._

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import repcheck.ingestion.votes.testing.{DockerRequired, TransactorFixture}
import repcheck.shared.models.congress.common.{Party, UsState}
import repcheck.shared.models.congress.dos.vote.VotePositionDO
import repcheck.shared.models.congress.vote.VoteCast

/**
 * Integration tests for [[DoobieVotePositionRepository]] against a real AlloyDB Omni container. Covers §6.3 AC rows
 * 8-10: replaceAll deletes old positions and inserts new ones; replaceAll handles an empty list; findByMemberAndBill
 * joins vote_positions to votes correctly.
 */
class DoobieVotePositionRepositorySpec extends AnyFlatSpec with Matchers with TransactorFixture {

  private lazy val repo = new DoobieVotePositionRepository

  private def makePosition(voteId: Long, memberId: Long, cast: VoteCast = VoteCast.Yea): VotePositionDO =
    VotePositionDO(
      voteId = voteId,
      memberId = memberId,
      position = Some(cast),
      partyAtVote = Some(Party.Democrat),
      stateAtVote = Some(UsState.NewYork),
      createdAt = None,
    )

  // AC#8
  "replaceAll" should "delete the existing positions and insert the new ones" taggedAs DockerRequired in {
    val voteId = insertVote("house:118:1:100", rollNumber = 100)
    val m1     = insertMember("A000001")
    val m2     = insertMember("A000002")
    val m3     = insertMember("A000003")
    val m4     = insertMember("A000004")
    val m5     = insertMember("A000005")
    val original = List(
      makePosition(voteId, m1),
      makePosition(voteId, m2),
      makePosition(voteId, m3),
      makePosition(voteId, m4),
      makePosition(voteId, m5),
    )
    repo.replaceAll(voteId, original).transact(xa).unsafeRunSync()

    val newMembers =
      List(insertMember("B000001"), insertMember("B000002"), insertMember("B000003"), insertMember("B000004"))
    val replacement = newMembers.map(m => makePosition(voteId, m, VoteCast.Nay))
    repo.replaceAll(voteId, replacement).transact(xa).unsafeRunSync()

    val found = repo.findByVoteId(voteId).transact(xa).unsafeRunSync()
    val _     = found.size shouldBe 4
    found.map(_.memberId).sorted shouldBe newMembers.sorted
  }

  // AC#9
  it should "delete all existing positions when the new list is empty" taggedAs DockerRequired in {
    val voteId   = insertVote("house:118:1:101", rollNumber = 101)
    val members  = (1 to 100).toList.map(i => insertMember(f"C$i%06d"))
    val original = members.map(m => makePosition(voteId, m))
    repo.replaceAll(voteId, original).transact(xa).unsafeRunSync()

    val _ = repo.findByVoteId(voteId).transact(xa).unsafeRunSync().size shouldBe 100

    repo.replaceAll(voteId, Nil).transact(xa).unsafeRunSync()

    val found = repo.findByVoteId(voteId).transact(xa).unsafeRunSync()
    found shouldBe empty
  }

  it should "respect the composite uniqueness constraint on (vote_id, member_id)" taggedAs DockerRequired in {
    val voteId    = insertVote("house:118:1:102", rollNumber = 102)
    val memberId  = insertMember("D000001")
    val duplicate = List(makePosition(voteId, memberId, VoteCast.Yea), makePosition(voteId, memberId, VoteCast.Nay))
    val outcome   = scala.util.Try(repo.replaceAll(voteId, duplicate).transact(xa).unsafeRunSync())
    // The unique constraint must reject the second row with the same (vote_id, member_id).
    outcome.isFailure shouldBe true
  }

  // AC#10
  "findByMemberAndBill" should "return every position a member cast on votes for a bill" taggedAs DockerRequired in {
    val billId   = insertBill(billNumber = 300)
    val vote1    = insertVote("house:118:1:103", rollNumber = 103, billId = Some(billId))
    val vote2    = insertVote("house:118:1:104", rollNumber = 104, billId = Some(billId))
    val memberId = insertMember("E000001")
    val other    = insertMember("E000002")

    repo
      .replaceAll(vote1, List(makePosition(vote1, memberId, VoteCast.Yea), makePosition(vote1, other, VoteCast.Nay)))
      .transact(xa)
      .unsafeRunSync()
    repo
      .replaceAll(vote2, List(makePosition(vote2, memberId, VoteCast.Nay)))
      .transact(xa)
      .unsafeRunSync()

    val found = repo.findByMemberAndBill(memberId, billId).transact(xa).unsafeRunSync()
    val _     = found.size shouldBe 2
    val _     = found.map(_.voteId).toSet shouldBe Set(vote1, vote2)
    found.forall(_.memberId == memberId) shouldBe true
  }

  it should "return empty when the member did not vote on any of the bill's votes" taggedAs DockerRequired in {
    val billId   = insertBill(billNumber = 301)
    val voteId   = insertVote("house:118:1:105", rollNumber = 105, billId = Some(billId))
    val memberId = insertMember("F000001")
    val other    = insertMember("F000002")
    repo.replaceAll(voteId, List(makePosition(voteId, other))).transact(xa).unsafeRunSync()

    val found = repo.findByMemberAndBill(memberId, billId).transact(xa).unsafeRunSync()
    found shouldBe empty
  }

}
