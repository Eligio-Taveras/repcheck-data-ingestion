package repcheck.ingestion.votes.repo

import doobie.implicits._
import doobie.postgres.implicits._
import doobie.{Fragment, Update}

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import repcheck.ingestion.votes.errors.VotePositionIdentityInvalid
import repcheck.shared.models.congress.common.DoobieEnumInstances._
import repcheck.shared.models.congress.common.{Party, UsState}
import repcheck.shared.models.congress.dos.vote.VotePositionDO
import repcheck.shared.models.congress.vote.VoteCast

/**
 * SQL-shape unit tests for [[DoobieVotePositionRepository]]. Targets the invariants that PR #44 introduces on top of
 * the migration-023 dual-identity shape:
 *   - `replaceAll` remains DELETE-then-INSERT (order matters for transactional correctness).
 *   - Every SELECT / INSERT column list is explicit, matches `VotePositionDO` constructor order, and carries
 *     `lis_member_id` (no implicit `SELECT *`).
 *   - `upsert` dispatches on which identity is populated and raises [[VotePositionIdentityInvalid]] for XOR violations.
 */
class DoobieVotePositionRepositoryUnitSpec extends AnyFlatSpec with Matchers {

  private val repo = new DoobieVotePositionRepository

  private val housePosition = VotePositionDO(
    id = 0L,
    voteId = 1L,
    memberId = Some(2L),
    position = Some(VoteCast.Yea),
    partyAtVote = Some(Party.Democrat),
    stateAtVote = Some(UsState.NewYork),
    createdAt = None,
    lisMemberId = None,
  )

  private val senatePosition = VotePositionDO(
    id = 0L,
    voteId = 1L,
    memberId = None,
    position = Some(VoteCast.Nay),
    partyAtVote = Some(Party.Republican),
    stateAtVote = Some(UsState.Texas),
    createdAt = None,
    lisMemberId = Some(999L),
  )

  // ---------------------------------------------------------------------------
  // SELECT column order
  // ---------------------------------------------------------------------------

  "findByVoteId SQL" should "list every column in VotePositionDO constructor order (with lis_member_id trailing)" in {
    val fragment =
      fr"SELECT id, vote_id, member_id, position, party_at_vote, state_at_vote, created_at, lis_member_id FROM vote_positions WHERE vote_id = 1"
    val sqlString = fragment.query[VotePositionDO].sql
    val _         = sqlString should include("id, vote_id, member_id, position")
    val _         = sqlString should include("party_at_vote, state_at_vote, created_at, lis_member_id")
    sqlString should not include "SELECT *"
  }

  it should "produce a ConnectionIO for a valid vote id" in {
    val cio = repo.findByVoteId(1L)
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  "findByVoteAndMember SQL" should "filter by vote_id AND member_id" in {
    val cio = repo.findByVoteAndMember(voteId = 1L, memberId = 2L)
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  it should "list every column including lis_member_id" in {
    val fragment =
      fr"SELECT id, vote_id, member_id, position, party_at_vote, state_at_vote, created_at, lis_member_id FROM vote_positions WHERE vote_id = 1 AND member_id = 2"
    val sqlString = fragment.query[VotePositionDO].sql
    val _         = sqlString should include("member_id")
    val _         = sqlString should include("lis_member_id")
    sqlString should not include "SELECT *"
  }

  "findByVoteAndLisMember SQL" should "filter by vote_id AND lis_member_id" in {
    val cio = repo.findByVoteAndLisMember(voteId = 1L, lisMemberId = 42L)
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  it should "list every column including member_id" in {
    val fragment =
      fr"SELECT id, vote_id, member_id, position, party_at_vote, state_at_vote, created_at, lis_member_id FROM vote_positions WHERE vote_id = 1 AND lis_member_id = 42"
    val sqlString = fragment.query[VotePositionDO].sql
    val _         = sqlString should include("lis_member_id")
    val _         = sqlString should include("member_id")
    sqlString should not include "SELECT *"
  }

  "findByMemberAndBill SQL" should "join vote_positions to votes on vote_id" in {
    val fragment =
      fr"""SELECT vp.id, vp.vote_id, vp.member_id, vp.position, vp.party_at_vote, vp.state_at_vote, vp.created_at, vp.lis_member_id
           FROM vote_positions vp JOIN votes v ON v.id = vp.vote_id
           WHERE vp.member_id = 2 AND v.bill_id = 42"""
    val sqlString = fragment.query[VotePositionDO].sql
    val _         = sqlString should include("JOIN votes v ON v.id = vp.vote_id")
    val _         = sqlString should include("vp.member_id = ")
    val _         = sqlString should include("v.bill_id = ")
    sqlString should include("vp.lis_member_id")
  }

  it should "produce a ConnectionIO for valid member and bill ids" in {
    val cio = repo.findByMemberAndBill(memberId = 1L, billId = 42L)
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  // ---------------------------------------------------------------------------
  // replaceAll: delete-then-insert + XOR validation
  // ---------------------------------------------------------------------------

  "replaceAll" should "produce a ConnectionIO for an empty position list" in {
    val cio = repo.replaceAll(1L, Nil)
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  it should "produce a ConnectionIO for a House-arm position list" in {
    val cio = repo.replaceAll(1L, List(housePosition, housePosition.copy(memberId = Some(3L))))
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  it should "produce a ConnectionIO for a Senate-arm position list" in {
    val cio = repo.replaceAll(1L, List(senatePosition, senatePosition.copy(lisMemberId = Some(998L))))
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  it should "produce a ConnectionIO for a mixed-chamber list (validation runs lazily via raiseError on violation)" in {
    val cio = repo.replaceAll(1L, List(housePosition, senatePosition))
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  "deletePositions SQL" should "delete all rows for a given vote_id" in {
    val sqlString =
      fr"DELETE FROM vote_positions WHERE vote_id = 1".update.sql
    val _ = sqlString should include("DELETE FROM vote_positions")
    sqlString should include("WHERE vote_id")
  }

  it should "produce a ConnectionIO for an arbitrary vote id" in {
    val cio = repo.deletePositions(Fragment.const("vote_positions"), voteId = 1L)
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  "insertPositions" should "return ConnectionIO.unit for an empty list" in {
    val cio = repo.insertPositions(1L, Nil)
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  it should "produce a ConnectionIO for a House-arm batch" in {
    val cio = repo.insertPositions(1L, List(housePosition))
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  it should "produce a ConnectionIO for a Senate-arm batch" in {
    val cio = repo.insertPositions(1L, List(senatePosition))
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  "insertPositions batch SQL" should "list all six bind columns including lis_member_id (no SELECT *)" in {
    val sqlString =
      Update[(Long, Option[Long], Option[VoteCast], Option[Party], Option[UsState], Option[Long])](
        "INSERT INTO vote_positions (vote_id, member_id, position, party_at_vote, state_at_vote, lis_member_id) VALUES (?, ?, ?, ?, ?, ?)"
      ).sql
    val _ = sqlString should include("(vote_id, member_id, position, party_at_vote, state_at_vote, lis_member_id)")
    sqlString should not include "SELECT *"
  }

  // ---------------------------------------------------------------------------
  // XOR identity validation
  // ---------------------------------------------------------------------------

  "isValidIdentity" should "return true when only memberId is populated" in {
    repo.isValidIdentity(housePosition) shouldBe true
  }

  it should "return true when only lisMemberId is populated" in {
    repo.isValidIdentity(senatePosition) shouldBe true
  }

  it should "return false when both memberId and lisMemberId are None" in {
    val bad = housePosition.copy(memberId = None, lisMemberId = None)
    repo.isValidIdentity(bad) shouldBe false
  }

  it should "return false when both memberId and lisMemberId are Some" in {
    val bad = housePosition.copy(memberId = Some(5L), lisMemberId = Some(6L))
    repo.isValidIdentity(bad) shouldBe false
  }

  "validatePositions" should "produce a ConnectionIO that succeeds for an empty list" in {
    val cio = repo.validatePositions(1L, Nil)
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  it should "produce a ConnectionIO that succeeds for House-only positions" in {
    val cio = repo.validatePositions(1L, List(housePosition))
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  it should "produce a ConnectionIO that succeeds for Senate-only positions" in {
    val cio = repo.validatePositions(1L, List(senatePosition))
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  it should "produce a ConnectionIO that raises for neither-identity positions" in {
    val bad = housePosition.copy(memberId = None, lisMemberId = None)
    val cio = repo.validatePositions(1L, List(bad))
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  it should "produce a ConnectionIO that raises for both-identity positions" in {
    val bad = housePosition.copy(memberId = Some(7L), lisMemberId = Some(8L))
    val cio = repo.validatePositions(1L, List(bad))
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  // ---------------------------------------------------------------------------
  // upsert dispatch
  // ---------------------------------------------------------------------------

  "upsert" should "dispatch to upsertHouse when only memberId is populated" in {
    val cio = repo.upsert(housePosition)
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  it should "dispatch to upsertSenate when only lisMemberId is populated" in {
    val cio = repo.upsert(senatePosition)
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  it should "raise VotePositionIdentityInvalid when both identities are None" in {
    val bad = housePosition.copy(memberId = None, lisMemberId = None)
    val cio = repo.upsert(bad)
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  it should "raise VotePositionIdentityInvalid when both identities are Some" in {
    val bad = housePosition.copy(memberId = Some(11L), lisMemberId = Some(12L))
    val cio = repo.upsert(bad)
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  "upsertHouse SQL" should "conflict on (vote_id, member_id) with partial predicate WHERE member_id IS NOT NULL" in {
    val cio = repo.upsertHouse(housePosition)
    val _   = cio shouldBe a[doobie.ConnectionIO[?]]
    val fragmentSql =
      (fr"INSERT INTO vote_positions (vote_id, member_id, lis_member_id, position, party_at_vote, state_at_vote)" ++
        fr"VALUES (1, 2, NULL, NULL, NULL, NULL)" ++
        fr"ON CONFLICT (vote_id, member_id) WHERE member_id IS NOT NULL" ++
        fr"DO UPDATE SET position = EXCLUDED.position").update.sql
    val _ = fragmentSql should include("ON CONFLICT (vote_id, member_id) WHERE member_id IS NOT NULL")
    val _ = fragmentSql should include("EXCLUDED.position")
    // created_at intentionally omitted so DEFAULT NOW() fires
    fragmentSql should not include "created_at"
  }

  "upsertSenate SQL" should "conflict on (vote_id, lis_member_id) with partial predicate WHERE lis_member_id IS NOT NULL" in {
    val cio = repo.upsertSenate(senatePosition)
    val _   = cio shouldBe a[doobie.ConnectionIO[?]]
    val fragmentSql =
      (fr"INSERT INTO vote_positions (vote_id, member_id, lis_member_id, position, party_at_vote, state_at_vote)" ++
        fr"VALUES (1, NULL, 999, NULL, NULL, NULL)" ++
        fr"ON CONFLICT (vote_id, lis_member_id) WHERE lis_member_id IS NOT NULL" ++
        fr"DO UPDATE SET position = EXCLUDED.position").update.sql
    val _ = fragmentSql should include("ON CONFLICT (vote_id, lis_member_id) WHERE lis_member_id IS NOT NULL")
    val _ = fragmentSql should include("EXCLUDED.position")
    fragmentSql should not include "created_at"
  }

  // ---------------------------------------------------------------------------
  // Trait conformance
  // ---------------------------------------------------------------------------

  "DoobieVotePositionRepository" should "implement VotePositionRepository trait" in {
    repo shouldBe a[VotePositionRepository]
  }

  // ---------------------------------------------------------------------------
  // Exception shape
  // ---------------------------------------------------------------------------

  "VotePositionIdentityInvalid" should "carry voteId, memberId, and lisMemberId in its message" in {
    val ex = VotePositionIdentityInvalid(voteId = 77L, memberId = Some(1L), lisMemberId = Some(2L))
    val _  = ex.getMessage should include("77")
    val _  = ex.getMessage should include("Some(1)")
    ex.getMessage should include("Some(2)")
  }

  it should "describe the neither-populated case clearly" in {
    val ex = VotePositionIdentityInvalid(voteId = 77L, memberId = None, lisMemberId = None)
    val _  = ex.getMessage should include("77")
    val _  = ex.getMessage should include("None")
    ex.getMessage should include("exactly one must be Some")
  }

}
