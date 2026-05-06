package repcheck.ingestion.amendments.testing

import java.time.Instant

import doobie.implicits._
import doobie.postgres.implicits._

import org.scalatest.Suite
import repcheck.members.common.testing.{TransactorFixture => SharedTransactorFixture}
import repcheck.shared.models.congress.common.BillType
import repcheck.shared.models.congress.common.DoobieEnumInstances._

/**
 * Amendments-pipeline integration-test fixture. Extends `members-common`'s [[SharedTransactorFixture]] to inherit the
 * shared AlloyDB Omni container, the Doobie transactor, and the members cleanup + insert helpers, and adds an
 * `insertBill` helper plus cleanup for the amendments + bills tables.
 *
 * ==Cleanup ordering==
 *
 * `afterEach` clears `amendments` (including its self-referencing parent FK) and `bills` BEFORE the members-common
 * super cleanup deletes `members` — `amendments.sponsor_member_id` and `bills` referenced by `amendments.bill_id` must
 * be released before the members rows they point at can be removed.
 *
 * Mirrors the votes-pipeline TransactorFixture pattern (extend members-common, override `afterEach` for child tables).
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

  /** Count rows in `amendments` matching a `natural_key`. Used by idempotency assertions. */
  protected def countAmendmentsByNaturalKey(naturalKey: String): Int =
    sql"""SELECT COUNT(*) FROM amendments WHERE natural_key = $naturalKey"""
      .query[Int]
      .unique
      .transact(xa)
      .unsafeRunSync()

  /**
   * Truncate `amendments` (a single DELETE — the table has self-referencing FKs via `parent_amendment_id`, so a single
   * statement releases all rows without ordering pitfalls) and `bills` before super-cleanup deletes the members the
   * amendments pointed at.
   */
  override def afterEach(): Unit = {
    val _ = sql"""DELETE FROM amendments""".update.run.transact(xa).unsafeRunSync()
    val _ = sql"""DELETE FROM bills""".update.run.transact(xa).unsafeRunSync()
    super.afterEach()
  }

}
