package repcheck.ingestion.votes.repo

import cats.effect.unsafe.implicits.global

import doobie.implicits._

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import repcheck.ingestion.votes.errors.VotePositionIdentityInvalid
import repcheck.ingestion.votes.testing.TransactorFixture
import repcheck.members.common.testing.DockerRequired
import repcheck.shared.models.congress.common.{Party, UsState}
import repcheck.shared.models.congress.dos.vote.VotePositionDO
import repcheck.shared.models.congress.vote.VoteCast

/**
 * Integration tests for [[DoobieVotePositionRepository]] against a real AlloyDB Omni container. Covers:
 *   - replaceAll for both arms (House via `member_id`, Senate via `lis_member_id`)
 *   - `upsert` dispatch: House → `uq_vp_vote_member`; Senate → `uq_vp_vote_lis`
 *   - XOR identity invariant enforcement at both the Scala layer ([[VotePositionIdentityInvalid]]) and the DB layer
 *     (`chk_vp_xor_identity` CHECK)
 *   - Partial UNIQUE index rejection for duplicate-identity inserts per arm
 *   - `findByVoteAndMember` / `findByVoteAndLisMember` dispatch
 *   - `findByMemberAndBill` retains House-only behavior
 */
class DoobieVotePositionRepositorySpec extends AnyFlatSpec with Matchers with TransactorFixture {

  private lazy val repo = new DoobieVotePositionRepository

  private def housePosition(voteId: Long, memberId: Long, cast: VoteCast = VoteCast.Yea): VotePositionDO =
    VotePositionDO(
      id = 0L,
      voteId = voteId,
      memberId = Some(memberId),
      position = Some(cast),
      partyAtVote = Some(Party.Democrat),
      stateAtVote = Some(UsState.NewYork),
      createdAt = None,
      lisMemberId = None,
    )

  private def senatePosition(voteId: Long, lisMemberId: Long, cast: VoteCast = VoteCast.Yea): VotePositionDO =
    VotePositionDO(
      id = 0L,
      voteId = voteId,
      memberId = None,
      position = Some(cast),
      partyAtVote = Some(Party.Republican),
      stateAtVote = Some(UsState.Texas),
      createdAt = None,
      lisMemberId = Some(lisMemberId),
    )

  /**
   * Insert a minimal `lis_members` row and return its auto-generated `id`. Used by Senate-arm specs which need a valid
   * FK target in `lis_members`.
   */
  private def insertLisMember(lisId: String): Long =
    sql"""INSERT INTO lis_members (natural_key)
          VALUES ($lisId)
          ON CONFLICT (natural_key) DO UPDATE SET natural_key = EXCLUDED.natural_key
          RETURNING id"""
      .query[Long]
      .unique
      .transact(xa)
      .unsafeRunSync()

  // ---------------------------------------------------------------------------
  // replaceAll — House arm
  // ---------------------------------------------------------------------------

  "replaceAll" should "delete the existing positions and insert the new ones (House arm)" taggedAs DockerRequired in {
    val voteId = insertVote("house:118:1:100", rollNumber = 100)
    val m1     = insertMember("A000001")
    val m2     = insertMember("A000002")
    val m3     = insertMember("A000003")
    val original = List(
      housePosition(voteId, m1),
      housePosition(voteId, m2),
      housePosition(voteId, m3),
    )
    repo.replaceAll(voteId, original).transact(xa).unsafeRunSync()

    val newMembers =
      List(insertMember("B000001"), insertMember("B000002"))
    val replacement = newMembers.map(m => housePosition(voteId, m, VoteCast.Nay))
    repo.replaceAll(voteId, replacement).transact(xa).unsafeRunSync()

    val found = repo.findByVoteId(voteId).transact(xa).unsafeRunSync()
    val _     = found.size shouldBe 2
    val _     = found.flatMap(_.memberId).sorted shouldBe newMembers.sorted
    found.forall(_.lisMemberId.isEmpty) shouldBe true
  }

  it should "delete all existing positions when the new list is empty" taggedAs DockerRequired in {
    val voteId   = insertVote("house:118:1:101", rollNumber = 101)
    val members  = (1 to 50).toList.map(i => insertMember(f"C$i%06d"))
    val original = members.map(m => housePosition(voteId, m))
    repo.replaceAll(voteId, original).transact(xa).unsafeRunSync()

    val _ = repo.findByVoteId(voteId).transact(xa).unsafeRunSync().size shouldBe 50

    repo.replaceAll(voteId, Nil).transact(xa).unsafeRunSync()

    val found = repo.findByVoteId(voteId).transact(xa).unsafeRunSync()
    found shouldBe empty
  }

  // ---------------------------------------------------------------------------
  // replaceAll — Senate arm
  // ---------------------------------------------------------------------------

  it should "insert Senate-arm rows with lis_member_id populated and member_id NULL" taggedAs DockerRequired in {
    val voteId = insertVote("senate:118:1:200", rollNumber = 200)
    val lis1   = insertLisMember("S000001")
    val lis2   = insertLisMember("S000002")
    val senateRows = List(
      senatePosition(voteId, lis1),
      senatePosition(voteId, lis2),
    )
    repo.replaceAll(voteId, senateRows).transact(xa).unsafeRunSync()

    val found = repo.findByVoteId(voteId).transact(xa).unsafeRunSync()
    val _     = found.size shouldBe 2
    val _     = found.flatMap(_.lisMemberId).sorted shouldBe List(lis1, lis2).sorted
    found.forall(_.memberId.isEmpty) shouldBe true
  }

  // ---------------------------------------------------------------------------
  // upsert — dispatch by identity
  // ---------------------------------------------------------------------------

  "upsert" should "INSERT a House-arm row with lis_member_id NULL" taggedAs DockerRequired in {
    val voteId = insertVote("house:118:1:300", rollNumber = 300)
    val m1     = insertMember("D000001")
    repo.upsert(housePosition(voteId, m1, VoteCast.Yea)).transact(xa).unsafeRunSync()

    val Some(row) = repo.findByVoteAndMember(voteId, m1).transact(xa).unsafeRunSync(): @unchecked
    val _         = row.memberId shouldBe Some(m1)
    val _         = row.lisMemberId shouldBe None
    row.position shouldBe Some(VoteCast.Yea)
  }

  it should "INSERT a Senate-arm row with member_id NULL" taggedAs DockerRequired in {
    val voteId = insertVote("senate:118:1:301", rollNumber = 301)
    val lisId  = insertLisMember("S000101")
    repo.upsert(senatePosition(voteId, lisId, VoteCast.Nay)).transact(xa).unsafeRunSync()

    val Some(row) = repo.findByVoteAndLisMember(voteId, lisId).transact(xa).unsafeRunSync(): @unchecked
    val _         = row.memberId shouldBe None
    val _         = row.lisMemberId shouldBe Some(lisId)
    row.position shouldBe Some(VoteCast.Nay)
  }

  it should "UPDATE position on second House upsert for the same (vote_id, member_id)" taggedAs DockerRequired in {
    val voteId = insertVote("house:118:1:302", rollNumber = 302)
    val m1     = insertMember("D000002")
    repo.upsert(housePosition(voteId, m1, VoteCast.Yea)).transact(xa).unsafeRunSync()
    repo.upsert(housePosition(voteId, m1, VoteCast.Nay)).transact(xa).unsafeRunSync()

    val Some(row) = repo.findByVoteAndMember(voteId, m1).transact(xa).unsafeRunSync(): @unchecked
    row.position shouldBe Some(VoteCast.Nay)
  }

  it should "UPDATE position on second Senate upsert for the same (vote_id, lis_member_id)" taggedAs DockerRequired in {
    val voteId = insertVote("senate:118:1:303", rollNumber = 303)
    val lisId  = insertLisMember("S000102")
    repo.upsert(senatePosition(voteId, lisId, VoteCast.Yea)).transact(xa).unsafeRunSync()
    repo.upsert(senatePosition(voteId, lisId, VoteCast.Nay)).transact(xa).unsafeRunSync()

    val Some(row) = repo.findByVoteAndLisMember(voteId, lisId).transact(xa).unsafeRunSync(): @unchecked
    row.position shouldBe Some(VoteCast.Nay)
  }

  it should "raise VotePositionIdentityInvalid when both identity columns are None" taggedAs DockerRequired in {
    val voteId  = insertVote("house:118:1:304", rollNumber = 304)
    val bad     = housePosition(voteId, 1L).copy(memberId = None, lisMemberId = None)
    val attempt = scala.util.Try(repo.upsert(bad).transact(xa).unsafeRunSync())
    attempt match {
      case scala.util.Failure(ex) => ex shouldBe a[VotePositionIdentityInvalid]
      case scala.util.Success(_)  => fail("expected VotePositionIdentityInvalid, got success")
    }
  }

  it should "raise VotePositionIdentityInvalid when both identity columns are Some" taggedAs DockerRequired in {
    val voteId  = insertVote("house:118:1:305", rollNumber = 305)
    val bad     = housePosition(voteId, 1L).copy(memberId = Some(99L), lisMemberId = Some(100L))
    val attempt = scala.util.Try(repo.upsert(bad).transact(xa).unsafeRunSync())
    attempt match {
      case scala.util.Failure(ex) => ex shouldBe a[VotePositionIdentityInvalid]
      case scala.util.Success(_)  => fail("expected VotePositionIdentityInvalid, got success")
    }
  }

  // ---------------------------------------------------------------------------
  // Partial UNIQUE index rejection
  // ---------------------------------------------------------------------------

  "replaceAll" should "be rejected by the partial UNIQUE uq_vp_vote_member when two House rows share (vote_id, member_id)" taggedAs DockerRequired in {
    val voteId   = insertVote("house:118:1:400", rollNumber = 400)
    val memberId = insertMember("E000001")
    val duplicate = List(
      housePosition(voteId, memberId, VoteCast.Yea),
      housePosition(voteId, memberId, VoteCast.Nay),
    )
    val outcome = scala.util.Try(repo.replaceAll(voteId, duplicate).transact(xa).unsafeRunSync())
    outcome.isFailure shouldBe true
  }

  it should "be rejected by the partial UNIQUE uq_vp_vote_lis when two Senate rows share (vote_id, lis_member_id)" taggedAs DockerRequired in {
    val voteId = insertVote("senate:118:1:401", rollNumber = 401)
    val lisId  = insertLisMember("S000201")
    val duplicate = List(
      senatePosition(voteId, lisId, VoteCast.Yea),
      senatePosition(voteId, lisId, VoteCast.Nay),
    )
    val outcome = scala.util.Try(repo.replaceAll(voteId, duplicate).transact(xa).unsafeRunSync())
    outcome.isFailure shouldBe true
  }

  it should "be rejected by the DB XOR CHECK when replaceAll-validation is bypassed via raw SQL" taggedAs DockerRequired in {
    val voteId = insertVote("house:118:1:402", rollNumber = 402)
    val bogusInsert =
      sql"""INSERT INTO vote_positions (vote_id, member_id, lis_member_id) VALUES ($voteId, NULL, NULL)""".update.run
    val outcome = scala.util.Try(bogusInsert.transact(xa).unsafeRunSync())
    outcome.isFailure shouldBe true
  }

  // ---------------------------------------------------------------------------
  // findByMemberAndBill — House-only semantics retained
  // ---------------------------------------------------------------------------

  "findByMemberAndBill" should "return every House-arm position a member cast on votes for a bill" taggedAs DockerRequired in {
    val billId   = insertBill(billNumber = 500)
    val vote1    = insertVote("house:118:1:500", rollNumber = 500, billId = Some(billId))
    val vote2    = insertVote("house:118:1:501", rollNumber = 501, billId = Some(billId))
    val memberId = insertMember("G000001")
    val other    = insertMember("G000002")

    repo
      .replaceAll(vote1, List(housePosition(vote1, memberId, VoteCast.Yea), housePosition(vote1, other, VoteCast.Nay)))
      .transact(xa)
      .unsafeRunSync()
    repo
      .replaceAll(vote2, List(housePosition(vote2, memberId, VoteCast.Nay)))
      .transact(xa)
      .unsafeRunSync()

    val found = repo.findByMemberAndBill(memberId, billId).transact(xa).unsafeRunSync()
    val _     = found.size shouldBe 2
    val _     = found.map(_.voteId).toSet shouldBe Set(vote1, vote2)
    found.forall(_.memberId.contains(memberId)) shouldBe true
  }

  it should "return empty when the member did not vote on any of the bill's votes" taggedAs DockerRequired in {
    val billId   = insertBill(billNumber = 501)
    val voteId   = insertVote("house:118:1:502", rollNumber = 502, billId = Some(billId))
    val memberId = insertMember("H000001")
    val other    = insertMember("H000002")
    repo.replaceAll(voteId, List(housePosition(voteId, other))).transact(xa).unsafeRunSync()

    val found = repo.findByMemberAndBill(memberId, billId).transact(xa).unsafeRunSync()
    found shouldBe empty
  }

  // ---------------------------------------------------------------------------
  // Dual-arm finder dispatch
  // ---------------------------------------------------------------------------

  "findByVoteAndMember" should "return None for a Senate-arm row even if its lis_member_id equals the supplied memberId" taggedAs DockerRequired in {
    val voteId = insertVote("senate:118:1:600", rollNumber = 600)
    val lisId  = insertLisMember("S000301")
    repo.upsert(senatePosition(voteId, lisId, VoteCast.Yea)).transact(xa).unsafeRunSync()

    val outcome = repo.findByVoteAndMember(voteId, lisId).transact(xa).unsafeRunSync()
    outcome shouldBe None
  }

  "findByVoteAndLisMember" should "return None for a House-arm row even if its member_id equals the supplied lisMemberId" taggedAs DockerRequired in {
    val voteId = insertVote("house:118:1:601", rollNumber = 601)
    val m1     = insertMember("I000001")
    repo.upsert(housePosition(voteId, m1, VoteCast.Yea)).transact(xa).unsafeRunSync()

    val outcome = repo.findByVoteAndLisMember(voteId, m1).transact(xa).unsafeRunSync()
    outcome shouldBe None
  }

}
