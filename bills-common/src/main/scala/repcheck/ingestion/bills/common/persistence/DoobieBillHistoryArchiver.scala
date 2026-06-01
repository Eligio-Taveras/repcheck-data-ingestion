package repcheck.ingestion.bills.common.persistence

import cats.syntax.all._

import doobie._
import doobie.implicits._

import repcheck.pipeline.models.constants.Tables

/**
 * Archive-before-overwrite pattern: snapshots the current bill state into history tables BEFORE the upsert writes new
 * data. The live bills table always holds the latest version; bill_history preserves each prior version. The caller is
 * responsible for calling archiveBill() before the upsert — the archiver does not modify the live bill row.
 */
class DoobieBillHistoryArchiver extends BillHistoryArchiver[ConnectionIO] {

  override def archiveBill(billId: String): ConnectionIO[Long] = {
    val (congress, billType, number) = parseNaturalKey(billId)
    val frags                        = buildFragments()
    insertBillHistory(congress, billType, number, frags).flatMap(archiveDependents(congress, billType, number, frags))
  }

  private[persistence] def archiveDependents(
    congress: Int,
    billType: String,
    number: String,
    f: ArchiveFragments,
  )(historyId: Long): ConnectionIO[Long] = {
    val cospCio = insertCosponsorHistory(historyId, congress, billType, number, f)
    val subjCio = insertSubjectHistory(historyId, congress, billType, number, f)
    cospCio *> subjCio.as(historyId)
  }

  private[persistence] case class ArchiveFragments(
    billsTable: Fragment,
    histTable: Fragment,
    cospHistTable: Fragment,
    subjHistTable: Fragment,
    cospTable: Fragment,
    subjTable: Fragment,
  )

  private[persistence] def buildFragments(): ArchiveFragments = ArchiveFragments(
    billsTable = Fragment.const(Tables.Bills),
    histTable = Fragment.const(HistoryTables.BillHistory),
    cospHistTable = Fragment.const(HistoryTables.BillCosponsorHistory),
    subjHistTable = Fragment.const(HistoryTables.BillSubjectHistory),
    cospTable = Fragment.const(Tables.BillCosponsors),
    subjTable = Fragment.const(Tables.BillSubjects),
  )

  private[persistence] def insertBillHistory(
    congress: Int,
    billType: String,
    number: String,
    f: ArchiveFragments,
  ): ConnectionIO[Long] =
    sql"""
      INSERT INTO ${f.histTable} (
        bill_id, congress, bill_type, number, title,
        origin_chamber, origin_chamber_code, introduced_date, policy_area,
        latest_action_date, latest_action_text, constitutional_authority_text,
        sponsor_member_id,
        update_date, update_date_including_text, legislation_url, api_url
      )
      SELECT
        id, congress, bill_type, number, title,
        origin_chamber, origin_chamber_code, introduced_date, policy_area,
        latest_action_date, latest_action_text, constitutional_authority_text,
        sponsor_member_id,
        update_date, update_date_including_text, legislation_url, api_url
      FROM ${f.billsTable}
      WHERE congress = $congress AND bill_type::text = $billType AND number = $number::int
      RETURNING id
    """.query[Long].unique

  private[persistence] def insertCosponsorHistory(
    historyId: Long,
    congress: Int,
    billType: String,
    number: String,
    f: ArchiveFragments,
  ): ConnectionIO[Int] =
    sql"""
      INSERT INTO ${f.cospHistTable} (history_id, bill_id, member_id, is_original_cosponsor, sponsorship_date)
      SELECT $historyId, c.bill_id, c.member_id, c.is_original_cosponsor, c.sponsorship_date
      FROM ${f.cospTable} c
      INNER JOIN ${f.billsTable} b ON c.bill_id = b.id
      WHERE b.congress = $congress AND b.bill_type::text = $billType AND b.number = $number::int
    """.update.run

  private[persistence] def insertSubjectHistory(
    historyId: Long,
    congress: Int,
    billType: String,
    number: String,
    f: ArchiveFragments,
  ): ConnectionIO[Int] =
    sql"""
      INSERT INTO ${f.subjHistTable} (history_id, bill_id, subject_name, update_date)
      SELECT $historyId, s.bill_id, s.subject_name, s.update_date
      FROM ${f.subjTable} s
      INNER JOIN ${f.billsTable} b ON s.bill_id = b.id
      WHERE b.congress = $congress AND b.bill_type::text = $billType AND b.number = $number::int
    """.update.run

  private[persistence] def parseNaturalKey(naturalKey: String): (Int, String, String) = {
    val parts = naturalKey.split("-", 3)
    (parts(0).toInt, parts(1).toLowerCase, parts(2))
  }

}
