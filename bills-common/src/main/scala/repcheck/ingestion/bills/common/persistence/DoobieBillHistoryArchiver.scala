package repcheck.ingestion.bills.common.persistence

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

    val billsTable    = Fragment.const(Tables.Bills)
    val histTable     = Fragment.const(HistoryTables.BillHistory)
    val cospHistTable = Fragment.const(HistoryTables.BillCosponsorHistory)
    val subjHistTable = Fragment.const(HistoryTables.BillSubjectHistory)
    val cospTable     = Fragment.const(Tables.BillCosponsors)
    val subjTable     = Fragment.const(Tables.BillSubjects)

    for {
      historyId <- sql"""
        INSERT INTO $histTable (
          bill_id, congress, bill_type, number, title,
          origin_chamber, origin_chamber_code, introduced_date, policy_area,
          latest_action_date, latest_action_text, constitutional_authority_text,
          sponsor_member_id, text_url, text_format, text_version_type, text_date,
          summary_text, summary_action_desc, summary_action_date,
          update_date, update_date_including_text, legislation_url, api_url
        )
        SELECT
          id, congress, bill_type, number, title,
          origin_chamber, origin_chamber_code, introduced_date, policy_area,
          latest_action_date, latest_action_text, constitutional_authority_text,
          sponsor_member_id, text_url, text_format, text_version_type, text_date,
          summary_text, summary_action_desc, summary_action_date,
          update_date, update_date_including_text, legislation_url, api_url
        FROM $billsTable
        WHERE congress = $congress AND bill_type::text = $billType AND number = $number::int
        RETURNING id
      """.query[Long].unique

      _ <- sql"""
        INSERT INTO $cospHistTable (history_id, bill_id, member_id, is_original_cosponsor, sponsorship_date)
        SELECT $historyId, c.bill_id, c.member_id, c.is_original_cosponsor, c.sponsorship_date
        FROM $cospTable c
        INNER JOIN $billsTable b ON c.bill_id = b.id
        WHERE b.congress = $congress AND b.bill_type::text = $billType AND b.number = $number::int
      """.update.run

      _ <- sql"""
        INSERT INTO $subjHistTable (history_id, bill_id, subject_name, update_date)
        SELECT $historyId, s.bill_id, s.subject_name, s.update_date
        FROM $subjTable s
        INNER JOIN $billsTable b ON s.bill_id = b.id
        WHERE b.congress = $congress AND b.bill_type::text = $billType AND b.number = $number::int
      """.update.run
    } yield historyId
  }

  private def parseNaturalKey(naturalKey: String): (Int, String, String) = {
    val parts = naturalKey.split("-", 3)
    (parts(0).toInt, parts(1).toLowerCase, parts(2))
  }

}
