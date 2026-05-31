package repcheck.ingestion.bills.common.persistence

import java.time.LocalDate

import cats.syntax.all._

import doobie._
import doobie.implicits._
import doobie.postgres.implicits._

import repcheck.pipeline.models.constants.Tables
import repcheck.shared.models.congress.bill.TextVersionCode
import repcheck.shared.models.congress.common.DoobieEnumInstances._

/**
 * Doobie implementation of [[BillSummaryRepository]]. The upsert resolves `bill_id` inline via an `INSERT ... SELECT id
 * FROM bills WHERE (congress, bill_type, number)` so the caller passes only the natural key — no separate id
 * round-trip. When the bill doesn't exist (caller forgot the placeholder), the SELECT yields no row and the statement
 * is a harmless no-op. `ON CONFLICT (bill_id, version_code)` makes re-fetches of the same stage idempotent.
 */
class DoobieBillSummaryRepository extends BillSummaryRepository[ConnectionIO] {

  private val table: Fragment      = Fragment.const(Tables.BillSummaries)
  private val billsTable: Fragment = Fragment.const(Tables.Bills)

  override def upsert(
    naturalKey: String,
    versionCode: TextVersionCode,
    text: String,
    actionDesc: Option[String],
    actionDate: Option[LocalDate],
  ): ConnectionIO[Unit] = {
    val parts    = naturalKey.split("-", 3)
    val congress = parts(0).toInt
    val billType = parts(1).toLowerCase
    val number   = parts(2)
    sql"""
      INSERT INTO $table (bill_id, version_code, action_date, action_desc, text)
      SELECT id, $versionCode, $actionDate, $actionDesc, $text
      FROM $billsTable
      WHERE congress = $congress AND bill_type::text = $billType AND number = $number::int
      ON CONFLICT (bill_id, version_code) DO UPDATE SET
        action_date = EXCLUDED.action_date,
        action_desc = EXCLUDED.action_desc,
        text = EXCLUDED.text,
        updated_at = NOW()
    """.update.run.void
  }

}
