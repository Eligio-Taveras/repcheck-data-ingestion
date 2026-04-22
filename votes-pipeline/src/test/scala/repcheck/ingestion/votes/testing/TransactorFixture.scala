package repcheck.ingestion.votes.testing

import java.time.{Instant, LocalDate}

import cats.effect.IO

import doobie.Transactor
import doobie.implicits._
import doobie.postgres.implicits._

import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Suite}
import repcheck.ingestion.votes.repo.VoteDoobieInstances._
import repcheck.shared.models.congress.common.DoobieEnumInstances._
import repcheck.shared.models.congress.common.{BillType, Chamber}
import repcheck.shared.models.congress.vote.{VoteMethod, VoteType}

/**
 * Provides a shared AlloyDB Omni container and Doobie transactor for Docker-backed integration tests in
 * `votes-pipeline`. All suites share one container (via [[SharedDockerPostgres]]) and run sequentially (`Test /
 * parallelExecution := false` in `build.sbt`) to avoid cross-suite FK violations during cleanup.
 */
trait TransactorFixture extends BeforeAndAfterAll with BeforeAndAfterEach { self: Suite =>

  import cats.effect.unsafe.implicits.global

  protected lazy val containerInfo: PostgresContainerInfo = SharedDockerPostgres.info

  protected lazy val xa: Transactor[IO] = Transactor.fromDriverManager[IO](
    driver = "org.postgresql.Driver",
    url = containerInfo.jdbcUrl,
    user = containerInfo.user,
    password = containerInfo.password,
    logHandler = None,
  )

  override def beforeAll(): Unit = {
    super.beforeAll()
    val _ = containerInfo
  }

  override def afterEach(): Unit = {
    cleanTables()
    super.afterEach()
  }

  /** Insert a minimal `members` row (placeholder OK) and return its auto-generated `id`. */
  protected def insertMember(bioguideId: String): Long =
    sql"""INSERT INTO members (natural_key)
          VALUES ($bioguideId)
          ON CONFLICT (natural_key) DO UPDATE SET natural_key = EXCLUDED.natural_key
          RETURNING id"""
      .query[Long]
      .unique
      .transact(xa)
      .unsafeRunSync()

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

  /** Fully reset every table that votes-pipeline specs write to. Order honors FK direction. */
  private def cleanTables(): Unit = {
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
    val _ = sql"""DELETE FROM members""".update.run.transact(xa).unsafeRunSync()
    // Senate-arm vote_positions FK to lis_members — clear any rows seeded by per-test helpers.
    val _ = sql"""DELETE FROM lis_members""".update.run.transact(xa).unsafeRunSync()
  }

}
