package com.repcheck.bills.common.persistence

import java.util.UUID

import cats.effect.kernel.MonadCancelThrow
import cats.syntax.all._

import doobie._
import doobie.implicits._
import doobie.postgres.implicits._

import repcheck.pipeline.models.constants.Tables

import com.repcheck.bills.common.errors.BillArchiveFailed

class DoobieBillHistoryArchiver[F[_]: MonadCancelThrow](xa: Transactor[F]) extends BillHistoryArchiver[F] {

  override def archiveBill(billId: String): F[UUID] = {
    val (congress, billType, number) = parseNaturalKey(billId)
    val historyId                    = UUID.randomUUID()

    val billsTable    = Fragment.const(Tables.Bills)
    val histTable     = Fragment.const(HistoryTables.BillHistory)
    val cospHistTable = Fragment.const(HistoryTables.BillCosponsorHistory)
    val subjHistTable = Fragment.const(HistoryTables.BillSubjectHistory)
    val cospTable     = Fragment.const(Tables.BillCosponsors)
    val subjTable     = Fragment.const(Tables.BillSubjects)

    val program = for {
      _ <- sql"""
        INSERT INTO $histTable (
          history_id, bill_id, congress, bill_type, number, title,
          origin_chamber, origin_chamber_code, introduced_date, policy_area,
          latest_action_date, latest_action_text, constitutional_authority_text,
          sponsor_bioguide_id, text_url, text_format, text_version_type, text_date,
          summary_text, summary_action_desc, summary_action_date,
          update_date, update_date_including_text, legislation_url, api_url
        )
        SELECT
          $historyId, id, congress, bill_type, number, title,
          origin_chamber, origin_chamber_code, introduced_date, policy_area,
          latest_action_date, latest_action_text, constitutional_authority_text,
          sponsor_bioguide_id, text_url, text_format, text_version_type, text_date,
          summary_text, summary_action_desc, summary_action_date,
          update_date, update_date_including_text, legislation_url, api_url
        FROM $billsTable
        WHERE congress = $congress AND bill_type = $billType AND number = $number::int
      """.update.run

      _ <- sql"""
        INSERT INTO $cospHistTable (history_id, bill_id, member_id, is_original_cosponsor, sponsorship_date)
        SELECT $historyId, c.bill_id, c.member_id, c.is_original_cosponsor, c.sponsorship_date
        FROM $cospTable c
        INNER JOIN $billsTable b ON c.bill_id = b.id
        WHERE b.congress = $congress AND b.bill_type = $billType AND b.number = $number::int
      """.update.run

      _ <- sql"""
        INSERT INTO $subjHistTable (history_id, bill_id, subject_name, update_date)
        SELECT $historyId, s.bill_id, s.subject_name, s.update_date
        FROM $subjTable s
        INNER JOIN $billsTable b ON s.bill_id = b.id
        WHERE b.congress = $congress AND b.bill_type = $billType AND b.number = $number::int
      """.update.run
    } yield historyId

    program.transact(xa).adaptErr {
      case e =>
        BillArchiveFailed(billId, e.getMessage, e)
    }
  }

  private def parseNaturalKey(naturalKey: String): (Int, String, String) = {
    val parts = naturalKey.split("-", 3)
    (parts(0).toInt, parts(1).toLowerCase, parts(2))
  }

}
