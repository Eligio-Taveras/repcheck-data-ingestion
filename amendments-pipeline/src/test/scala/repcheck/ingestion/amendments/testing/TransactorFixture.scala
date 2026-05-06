package repcheck.ingestion.amendments.testing

import java.time.{Instant, LocalDate}

import cats.effect.IO

import doobie.Transactor
import doobie.implicits._
import doobie.postgres.implicits._

import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Suite}
import repcheck.shared.models.congress.common.BillType
import repcheck.shared.models.congress.common.DoobieEnumInstances._

/**
 * Provides the shared AlloyDB Omni container + Doobie transactor for amendments-pipeline integration tests. All suites
 * share one container (via [[SharedDockerPostgres]]) and run sequentially (`Test / parallelExecution := false` in
 * `build.sbt`) to avoid cross-suite FK violations during cleanup.
 *
 * Helper methods (`insertMember`, `insertBill`) seed parent rows tests need to satisfy `amendments` foreign keys
 * (`bill_id` → `bills.id`, `sponsor_member_id` → `members.id`, `parent_amendment_id` → `amendments.id`).
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

  /**
   * Insert a `members` row (minimum-viable) and return its auto-generated `id`. The amendments-pipeline tests need a
   * real `members.id` to satisfy `amendments.sponsor_member_id_fkey`. We bind only the required `natural_key` column
   * and let DB defaults fill the rest.
   */
  protected def insertMember(naturalKey: String): Long =
    sql"""INSERT INTO members (natural_key) VALUES ($naturalKey)
          ON CONFLICT (natural_key) DO UPDATE SET natural_key = EXCLUDED.natural_key
          RETURNING id"""
      .query[Long]
      .unique
      .transact(xa)
      .unsafeRunSync()

  /**
   * Insert a `bills` row and return its auto-generated `id`. Mirrors the votes-pipeline TransactorFixture.insertBill —
   * keeps test data minimal so we exercise the amendments repository in isolation. `update_date` is required because
   * downstream archival paths still demand a non-null value (per migration 012's relaxation policy on the live table
   * but not the history mirror — irrelevant here, we just need the column populated).
   */
  protected def insertBill(
    billNumber: Int,
    billType: BillType = BillType.HR,
    congress: Int = 118,
  ): Long = {
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
   * Insert a fully populated `amendments` row directly via Doobie and return its auto-generated `id`. Useful as a
   * non-repository ground truth when a test needs a parent row for `parent_amendment_id` FK without going through the
   * production code path under test.
   */
  protected def insertAmendmentRow(
    naturalKey: String,
    congress: Int = 118,
    amendmentTypeApiValue: String = "samdt",
    number: String = "1",
    chamberApiValue: String = "Senate",
    billId: Option[Long] = None,
    sponsorMemberId: Option[Long] = None,
    parentAmendmentId: Option[Long] = None,
    submittedDate: Option[Instant] = None,
    proposedDate: Option[LocalDate] = None,
    latestActionDate: Option[LocalDate] = None,
    latestActionTime: Option[String] = None,
    latestActionText: Option[String] = None,
    updateDate: Option[Instant] = Some(Instant.parse("2024-01-15T00:00:00Z")),
    apiUrl: Option[String] = None,
    description: Option[String] = None,
    purpose: Option[String] = None,
    lastTextCheckAt: Option[Instant] = None,
  ): Long =
    sql"""INSERT INTO amendments (
            natural_key, congress, amendment_type, number, bill_id, chamber,
            description, purpose, sponsor_member_id,
            submitted_date, proposed_date, latest_action_date, latest_action_time, latest_action_text,
            update_date, api_url, parent_amendment_id, last_text_check_at
          ) VALUES (
            $naturalKey, $congress, $amendmentTypeApiValue::amendment_type_enum, $number, $billId,
            $chamberApiValue::chamber_type, $description, $purpose, $sponsorMemberId,
            $submittedDate, $proposedDate, $latestActionDate, $latestActionTime, $latestActionText,
            $updateDate, $apiUrl, $parentAmendmentId, $lastTextCheckAt
          )
          RETURNING id"""
      .query[Long]
      .unique
      .transact(xa)
      .unsafeRunSync()

  /** Count rows in `amendments` matching a `natural_key`. Used by idempotency assertions. */
  protected def countAmendmentsByNaturalKey(naturalKey: String): Int =
    sql"""SELECT COUNT(*) FROM amendments WHERE natural_key = $naturalKey"""
      .query[Int]
      .unique
      .transact(xa)
      .unsafeRunSync()

  /**
   * Truncate amendments and the FK parents in dependency order. `amendments` has self-referencing FKs
   * (parent_amendment_id), so we issue a single DELETE without a WHERE — that releases all rows in one statement and
   * avoids the tail-rows-with-children ordering pitfall. Bills/members get cleared after so their referenced surrogate
   * ids vanish along with the amendments that pointed at them.
   */
  private def cleanTables(): Unit = {
    val _ = sql"""
      DELETE FROM amendments;
      DELETE FROM bills;
      DELETE FROM members;
    """.update.run.transact(xa).unsafeRunSync()
  }

}
