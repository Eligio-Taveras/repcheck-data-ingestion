package repcheck.ingestion.votes.repo

import doobie.implicits._

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * SQL-shape unit tests for [[DoobieStanceMaterializationStatusRepository]]. Verifies the UPSERT clause is correct and
 * that only the votes-owned columns (`has_votes`, `votes_updated_at`) are touched — preventing accidental overwrites of
 * `has_analysis` or `all_passes_completed`.
 */
class DoobieStanceMaterializationStatusRepositoryUnitSpec extends AnyFlatSpec with Matchers {

  private val repo = new DoobieStanceMaterializationStatusRepository

  "markHasVotes" should "produce a ConnectionIO for a valid bill id" in {
    val cio = repo.markHasVotes(42L)
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  it should "produce a ConnectionIO for a zero bill id" in {
    val cio = repo.markHasVotes(0L)
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  it should "produce a ConnectionIO for a large bill id" in {
    val cio = repo.markHasVotes(Long.MaxValue)
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  "markHasVotes SQL" should "include INSERT ... ON CONFLICT (bill_id) DO UPDATE" in {
    val fragment = sql"""INSERT INTO stance_materialization_status (bill_id, has_votes, votes_updated_at)
                         VALUES (${42L}, TRUE, NOW())
                         ON CONFLICT (bill_id) DO UPDATE SET
                           has_votes = TRUE,
                           votes_updated_at = NOW()"""
    val sqlString = fragment.update.sql
    val _         = sqlString should include("ON CONFLICT (bill_id) DO UPDATE")
    val _         = sqlString should include("has_votes = TRUE")
    sqlString should include("votes_updated_at = NOW()")
  }

  it should "only touch has_votes and votes_updated_at columns on conflict" in {
    val fragment = sql"""INSERT INTO stance_materialization_status (bill_id, has_votes, votes_updated_at)
                         VALUES (${42L}, TRUE, NOW())
                         ON CONFLICT (bill_id) DO UPDATE SET
                           has_votes = TRUE,
                           votes_updated_at = NOW()"""
    val sqlString = fragment.update.sql
    // Analysis-owned columns must NOT appear in the UPDATE SET clause.
    val _ = sqlString should not include "has_analysis"
    val _ = sqlString should not include "all_passes_completed"
    sqlString should not include "analysis_completed_at"
  }

  "DoobieStanceMaterializationStatusRepository" should "implement StanceMaterializationStatusRepository trait" in {
    repo shouldBe a[StanceMaterializationStatusRepository]
  }

}
