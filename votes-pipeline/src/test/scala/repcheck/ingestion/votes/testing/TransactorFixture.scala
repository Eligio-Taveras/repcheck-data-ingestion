package repcheck.ingestion.votes.testing

import java.time.{Instant, LocalDate}

import doobie.implicits._
import doobie.postgres.implicits._

import org.scalatest.Suite
import repcheck.ingestion.votes.repo.VoteDoobieInstances._
import repcheck.members.common.testing.{TransactorFixture => SharedTransactorFixture}
import repcheck.shared.models.congress.common.DoobieEnumInstances._
import repcheck.shared.models.congress.common.{BillType, Chamber}
import repcheck.shared.models.congress.vote.{VoteMethod, VoteType}

/**
 * Votes-pipeline integration-test fixture. Extends `members-common`'s [[SharedTransactorFixture]] to inherit the shared
 * AlloyDB Omni container, the Doobie transactor, and the members/lis-members cleanup + insert helpers, and adds
 * vote-specific helpers (`insertVote`, `insertBill`) plus cleanup for the vote family of tables.
 *
 * ==Cleanup ordering==
 *
 * `afterEach` first truncates the vote family (vote_history_positions → vote_history → vote_positions → votes →
 * stance_materialization_status → bills) so the members-common super cleanup can then delete members/lis_members
 * without FK violations — `vote_positions.member_id` and `vote_positions.lis_member_id` are FKs to `members` and
 * `lis_members` respectively.
 */
trait TransactorFixture extends SharedTransactorFixture { self: Suite =>

  import cats.effect.unsafe.implicits.global

  /**
   * Insert a minimal `bills` row and return its auto-generated `id`. The `bills` table has no single natural-key column
   * — the uniqueness constraint is on `(congress, bill_type, number)`. We accept a `billNumber` parameter so multiple
   * tests can avoid colliding on the default value.
   */
  protected def insertBill(billNumber: Int, billType: BillType = BillType.HR, congress: Int = 118): Long = {
    val now = Instant.parse("2024-01-15T00:00:00Z")
    sql"""INSERT INTO bills (congress, bill_type, number, title, update_date)
          VALUES ($congress, $billType, $billNumber, 'Test bill', $now)
          ON CONFLICT (congress, bill_type, number) DO UPDATE SET update_date = EXCLUDED.update_date
          RETURNING id"""
      .query[Long]
      .unique
      .transact(xa)
      .unsafeRunSync()
  }

  /**
   * Insert a `votes` row via Doobie so tests have a parent row for `vote_positions` without relying on the production
   * repository (which is also under test here). Returns the auto-generated `id`.
   */
  protected def insertVote(
    naturalKey: String,
    congress: Int = 118,
    chamber: Chamber = Chamber.House,
    rollNumber: Int = 1,
    sessionNumber: Option[Int] = Some(1),
    billId: Option[Long] = None,
    voteType: Option[VoteType] = Some(VoteType.Passage),
    voteMethod: Option[VoteMethod] = Some(VoteMethod.YeaAndNay),
    voteDate: Option[LocalDate] = Some(LocalDate.parse("2024-01-15")),
    updateDate: Option[Instant] = Some(Instant.parse("2024-01-15T00:00:00Z")),
  ): Long = {
    // NB: archiver tests insert data that is later copied into `vote_history`, which still has
    // NOT NULL constraints on `question`, `vote_type`, `result`, `vote_date`, and `update_date`
    // (only the live `votes` table was relaxed by migration 012). Populate those columns here
    // so archive INSERT-FROM-SELECT stays within `vote_history` schema.
    val question = Some("On Passage")
    val result   = Some("Passed")
    sql"""INSERT INTO votes (
            natural_key, congress, chamber, roll_number, session_number, bill_id,
            question, vote_type, vote_method, result, vote_date, legislation_number,
            legislation_type, legislation_url, source_data_url, update_date
          ) VALUES (
            $naturalKey, $congress, $chamber, $rollNumber, $sessionNumber, $billId,
            $question, $voteType, $voteMethod, $result,
            $voteDate, ${Option.empty[String]}, ${Option.empty[BillType]},
            ${Option.empty[String]}, ${Option.empty[String]}, $updateDate
          )
          ON CONFLICT (natural_key) DO UPDATE SET
            congress = EXCLUDED.congress,
            update_date = EXCLUDED.update_date,
            updated_at = NOW()
          RETURNING id"""
      .query[Long]
      .unique
      .transact(xa)
      .unsafeRunSync()
  }

  /**
   * Clear every vote-family table before `super.afterEach` deletes members / lis_members. Order honors FK direction:
   * vote_history_positions → vote_history → vote_positions → votes → stance_materialization_status → bills.
   */
  override def afterEach(): Unit = {
    val _ = sql"""TRUNCATE TABLE vote_history_positions RESTART IDENTITY CASCADE""".update.run
      .transact(xa)
      .unsafeRunSync()
    val _ = sql"""TRUNCATE TABLE vote_history RESTART IDENTITY CASCADE""".update.run
      .transact(xa)
      .unsafeRunSync()
    val _ = sql"""TRUNCATE TABLE vote_positions RESTART IDENTITY CASCADE""".update.run
      .transact(xa)
      .unsafeRunSync()
    val _ = sql"""TRUNCATE TABLE votes RESTART IDENTITY CASCADE""".update.run
      .transact(xa)
      .unsafeRunSync()
    val _ = sql"""TRUNCATE TABLE stance_materialization_status RESTART IDENTITY CASCADE""".update.run
      .transact(xa)
      .unsafeRunSync()
    val _ = sql"""DELETE FROM bills""".update.run.transact(xa).unsafeRunSync()
    super.afterEach()
  }

}
