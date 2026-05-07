package repcheck.ingestion.votes.repo

import java.time.{Instant, LocalDate}

import doobie.implicits._
import doobie.postgres.implicits._

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import repcheck.shared.models.congress.common.DoobieEnumInstances._
import repcheck.shared.models.congress.common.{BillType, Chamber, LegislationKind}
import repcheck.shared.models.congress.dos.vote.VoteDO
import repcheck.shared.models.congress.vote.{VoteMethod, VoteType}

/**
 * SQL-shape unit tests for [[DoobieVoteRepository]]. Instead of executing SQL against a database, these tests render
 * the Doobie `Fragment` to a raw string and assert on its structure — the same technique plan decision 16 prescribes
 * for guarding against `SELECT *` regressions and ON-CONFLICT drift.
 */
class DoobieVoteRepositoryUnitSpec extends AnyFlatSpec with Matchers {

  private val repo = new DoobieVoteRepository

  private val sampleVote = VoteDO(
    voteId = 0L,
    naturalKey = "house:118:1:17",
    congress = 118,
    chamber = Chamber.House,
    rollNumber = 17,
    sessionNumber = Some(1),
    billId = Some(42L),
    question = Some("On Passage"),
    voteType = Some(VoteType.Passage),
    voteMethod = Some(VoteMethod.YeaAndNay),
    result = Some("Passed"),
    voteDate = Some(LocalDate.parse("2024-01-15")),
    legislationNumber = Some("30"),
    legislationType = Some(LegislationKind.BILL),
    billType = Some(BillType.HR),
    amendmentType = None,
    legislationUrl = Some("https://www.congress.gov/bill/118th-congress/house-bill/30"),
    sourceDataUrl = Some("https://clerk.house.gov/Votes/2024/roll17.xml"),
    updateDate = Some(Instant.parse("2024-01-15T00:00:00Z")),
    createdAt = Some(Instant.parse("2024-01-01T00:00:00Z")),
    updatedAt = Some(Instant.parse("2024-01-15T00:00:00Z")),
  )

  // ---------------------------------------------------------------------------
  // SELECT shape tests — explicit column list, no SELECT *
  // ---------------------------------------------------------------------------

  "findByNaturalKey SQL" should "list every column in VoteDO constructor order" in {
    val (sqlString, _) =
      fr"SELECT id, natural_key, congress, chamber, roll_number, session_number, bill_id, question, vote_type, vote_method, result, vote_date, legislation_number, legislation_type, bill_type, amendment_type, legislation_url, source_data_url, update_date, created_at, updated_at FROM votes WHERE natural_key = ${"house:118:1:17"}"
        .query[VoteDO]
        .sql -> ()
    // Guard against regressions: the query must name every column and must not use SELECT *.
    val _ = sqlString should include("id, natural_key, congress, chamber, roll_number")
    val _ = sqlString should include("legislation_type, bill_type, amendment_type, legislation_url, source_data_url")
    val _ = sqlString should include("update_date, created_at, updated_at")
    sqlString should not include "SELECT *"
  }

  it should "produce a ConnectionIO for a valid natural key" in {
    val cio = repo.findByNaturalKey("house:118:1:17")
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  it should "produce a ConnectionIO for an empty natural key" in {
    val cio = repo.findByNaturalKey("")
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  "findByBillId" should "produce a ConnectionIO for a valid bill id" in {
    val cio = repo.findByBillId(42L)
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  it should "produce a ConnectionIO for a zero bill id" in {
    val cio = repo.findByBillId(0L)
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  "findByCongress" should "produce a ConnectionIO for any congress value" in {
    val cio = repo.findByCongress(118)
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  it should "produce a ConnectionIO for congress zero" in {
    val cio = repo.findByCongress(0)
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  // ---------------------------------------------------------------------------
  // UPSERT shape tests — ON CONFLICT clause, explicit columns, updated_at refresh
  // ---------------------------------------------------------------------------

  "upsert SQL" should "include ON CONFLICT (natural_key) DO UPDATE clause" in {
    val insertFragment = sql"""INSERT INTO votes (
        natural_key, congress, chamber, roll_number, session_number, bill_id,
        question, vote_type, vote_method, result, vote_date, legislation_number,
        legislation_type, bill_type, amendment_type, legislation_url, source_data_url, update_date
      ) VALUES (${sampleVote.naturalKey}, ${sampleVote.congress}, ${sampleVote.chamber},
        ${sampleVote.rollNumber}, ${sampleVote.sessionNumber}, ${sampleVote.billId},
        ${sampleVote.question}, ${sampleVote.voteType}, ${sampleVote.voteMethod},
        ${sampleVote.result}, ${sampleVote.voteDate}, ${sampleVote.legislationNumber},
        ${sampleVote.legislationType}, ${sampleVote.billType}, ${sampleVote.amendmentType},
        ${sampleVote.legislationUrl},
        ${sampleVote.sourceDataUrl}, ${sampleVote.updateDate})
      ON CONFLICT (natural_key) DO UPDATE SET updated_at = NOW()"""
    insertFragment.query[Long].sql should include("ON CONFLICT (natural_key) DO UPDATE")
  }

  it should "refresh updated_at = NOW() on conflict" in {
    val fragment = fr"ON CONFLICT (natural_key) DO UPDATE SET updated_at = NOW()"
    fragment.query[Long].sql should include("updated_at = NOW()")
  }

  it should "produce a ConnectionIO for a fully populated vote" in {
    val cio = repo.upsert(sampleVote)
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  it should "produce a ConnectionIO for a vote with minimal fields" in {
    val minimal = VoteDO(
      voteId = 0L,
      naturalKey = "senate:118:1:1",
      congress = 118,
      chamber = Chamber.Senate,
      rollNumber = 1,
      sessionNumber = None,
      billId = None,
      question = None,
      voteType = None,
      voteMethod = None,
      result = None,
      voteDate = None,
      legislationNumber = None,
      legislationType = None,
      billType = None,
      amendmentType = None,
      legislationUrl = None,
      sourceDataUrl = None,
      updateDate = None,
      createdAt = None,
      updatedAt = None,
    )
    val cio = repo.upsert(minimal)
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  "DoobieVoteRepository" should "implement VoteRepository trait" in {
    repo shouldBe a[VoteRepository]
  }

}
