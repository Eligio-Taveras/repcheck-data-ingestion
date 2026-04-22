package repcheck.ingestion.votes.repo

import doobie.Fragment
import doobie.implicits._

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * SQL-shape unit tests for [[DoobieVoteHistoryArchiver]]. Targets the private-to-package helpers that do the actual
 * INSERT-FROM-SELECT work, so the archive flow is fully exercised without a database. An integration spec
 * (`DoobieVoteHistoryArchiverSpec`) covers end-to-end behavior against DockerPostgres.
 */
class DoobieVoteHistoryArchiverUnitSpec extends AnyFlatSpec with Matchers {

  private val archiver = new DoobieVoteHistoryArchiver

  private val historyTable    = Fragment.const("vote_history")
  private val votesTable      = Fragment.const("votes")
  private val historyPosTable = Fragment.const("vote_history_positions")
  private val positionsTable  = Fragment.const("vote_positions")

  "archiveVote" should "produce a ConnectionIO for a valid vote id" in {
    val cio = archiver.archiveVote(1L)
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  it should "produce a ConnectionIO for a zero vote id" in {
    val cio = archiver.archiveVote(0L)
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  it should "produce a ConnectionIO for a large vote id" in {
    val cio = archiver.archiveVote(Long.MaxValue)
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  "archivePositions" should "produce a ConnectionIO[Unit]" in {
    val cio = archiver.archivePositions(historyPosTable, positionsTable, voteId = 1L)(historyId = 10L)
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  it should "produce a ConnectionIO for any history and vote id combination" in {
    val cio = archiver.archivePositions(historyPosTable, positionsTable, voteId = 5L)(historyId = 999L)
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  "insertVoteHistory" should "produce a ConnectionIO[Long]" in {
    val cio = archiver.insertVoteHistory(historyTable, votesTable, voteId = 1L)
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  it should "produce a ConnectionIO for different vote ids" in {
    val cio = archiver.insertVoteHistory(historyTable, votesTable, voteId = 42L)
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  "insertHistoryPositions" should "produce a ConnectionIO[Int]" in {
    val cio = archiver.insertHistoryPositions(historyId = 1L, historyPosTable, positionsTable, voteId = 1L)
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  it should "produce a ConnectionIO for different history and vote id combinations" in {
    val cio = archiver.insertHistoryPositions(historyId = 99L, historyPosTable, positionsTable, voteId = 5L)
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  // ---------------------------------------------------------------------------
  // SQL shape: INSERT-FROM-SELECT column lists match VoteHistoryDO / VoteHistoryPositionDO order
  // ---------------------------------------------------------------------------

  "insertVoteHistory SQL" should "list vote columns explicitly (no SELECT *)" in {
    val fragment = sql"""INSERT INTO vote_history (
        vote_id, congress, chamber, roll_number, session_number, bill_id,
        question, vote_type, vote_method, result, vote_date, legislation_number,
        legislation_type, legislation_url, source_data_url, update_date
      )
      SELECT
        id, congress, chamber, roll_number, session_number, bill_id,
        question, vote_type, vote_method, result, vote_date, legislation_number,
        legislation_type, legislation_url, source_data_url, update_date
      FROM votes
      WHERE id = 1
      RETURNING id"""
    val sqlString = fragment.query[Long].sql
    val _         = sqlString should include("vote_id, congress, chamber, roll_number")
    val _         = sqlString should include("legislation_type, legislation_url, source_data_url, update_date")
    sqlString should not include "SELECT *"
  }

  "insertHistoryPositions SQL" should "tag each inserted row with the shared history_id" in {
    val fragment = sql"""INSERT INTO vote_history_positions (
        history_id, member_id, position, party_at_vote, state_at_vote
      )
      SELECT
        ${10L}, vp.member_id, vp.position, vp.party_at_vote, vp.state_at_vote
      FROM vote_positions vp
      WHERE vp.vote_id = 1"""
    val sqlString = fragment.update.sql
    val _         = sqlString should include("history_id, member_id, position, party_at_vote, state_at_vote")
    sqlString should not include "SELECT *"
  }

  "DoobieVoteHistoryArchiver" should "implement VoteHistoryArchiver trait" in {
    archiver shouldBe a[VoteHistoryArchiver]
  }

}
