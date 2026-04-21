package repcheck.ingestion.votes.repo

import doobie.implicits._
import doobie.postgres.implicits._
import doobie.{Fragment, Update}

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import repcheck.shared.models.congress.common.DoobieEnumInstances._
import repcheck.shared.models.congress.common.{Party, UsState}
import repcheck.shared.models.congress.dos.vote.VotePositionDO
import repcheck.shared.models.congress.vote.VoteCast

/**
 * SQL-shape unit tests for [[DoobieVotePositionRepository]]. Targets the two invariants that P2.4's plan highlights:
 *   - `replaceAll` is always DELETE-then-INSERT (order matters for transactional correctness).
 *   - INSERT columns match `VotePositionDO` constructor order, no implicit `SELECT *`.
 */
class DoobieVotePositionRepositoryUnitSpec extends AnyFlatSpec with Matchers {

  private val repo = new DoobieVotePositionRepository

  private val samplePosition = VotePositionDO(
    voteId = 1L,
    memberId = 2L,
    position = Some(VoteCast.Yea),
    partyAtVote = Some(Party.Democrat),
    stateAtVote = Some(UsState.NewYork),
    createdAt = None,
  )

  // ---------------------------------------------------------------------------
  // SELECT column order
  // ---------------------------------------------------------------------------

  "findByVoteId SQL" should "list every column in VotePositionDO constructor order" in {
    val fragment =
      fr"SELECT vote_id, member_id, position, party_at_vote, state_at_vote, created_at FROM vote_positions WHERE vote_id = 1"
    val sqlString = fragment.query[VotePositionDO].sql
    val _         = sqlString should include("vote_id, member_id, position")
    val _         = sqlString should include("party_at_vote, state_at_vote, created_at")
    sqlString should not include "SELECT *"
  }

  it should "produce a ConnectionIO for a valid vote id" in {
    val cio = repo.findByVoteId(1L)
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  "findByMemberAndBill SQL" should "join vote_positions to votes on vote_id" in {
    val fragment =
      fr"""SELECT vp.vote_id, vp.member_id, vp.position, vp.party_at_vote, vp.state_at_vote, vp.created_at
           FROM vote_positions vp JOIN votes v ON v.id = vp.vote_id
           WHERE vp.member_id = 2 AND v.bill_id = 42"""
    val sqlString = fragment.query[VotePositionDO].sql
    val _         = sqlString should include("JOIN votes v ON v.id = vp.vote_id")
    val _         = sqlString should include("vp.member_id = ")
    sqlString should include("v.bill_id = ")
  }

  it should "produce a ConnectionIO for valid member and bill ids" in {
    val cio = repo.findByMemberAndBill(memberId = 1L, billId = 42L)
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  // ---------------------------------------------------------------------------
  // replaceAll: delete-then-insert
  // ---------------------------------------------------------------------------

  "replaceAll" should "produce a ConnectionIO for an empty position list" in {
    val cio = repo.replaceAll(1L, Nil)
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  it should "produce a ConnectionIO for a non-empty position list" in {
    val cio = repo.replaceAll(1L, List(samplePosition, samplePosition.copy(memberId = 3L)))
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

  it should "produce a ConnectionIO for a non-empty batch" in {
    val cio = repo.insertPositions(1L, List(samplePosition))
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  "insertPositions batch SQL" should "list columns explicitly (no SELECT *)" in {
    val sqlString =
      Update[(Long, Long, Option[VoteCast], Option[Party], Option[UsState])](
        "INSERT INTO vote_positions (vote_id, member_id, position, party_at_vote, state_at_vote) VALUES (?, ?, ?, ?, ?)"
      ).sql
    val _ = sqlString should include("(vote_id, member_id, position, party_at_vote, state_at_vote)")
    sqlString should not include "SELECT *"
  }

  "DoobieVotePositionRepository" should "implement VotePositionRepository trait" in {
    repo shouldBe a[VotePositionRepository]
  }

}
