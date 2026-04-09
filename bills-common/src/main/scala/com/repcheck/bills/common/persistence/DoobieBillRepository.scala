package com.repcheck.bills.common.persistence

import java.util.UUID

import cats.effect.kernel.MonadCancelThrow
import cats.syntax.all._

import doobie._
import doobie.implicits._
import doobie.postgres.implicits._

import repcheck.pipeline.models.constants.Tables
import repcheck.shared.models.congress.dos.bill.BillDO

import com.repcheck.bills.common.errors.BillUpsertFailed
import com.repcheck.bills.common.persistence.DoobieInstances.{floatArrayGet, floatArrayPut}

class DoobieBillRepository[F[_]: MonadCancelThrow](xa: Transactor[F]) extends BillRepository[F] {

  private val table = Fragment.const(Tables.Bills)

  private val selectColumns: Fragment = fr"""
    id,
    (congress::text || '-' || UPPER(bill_type) || '-' || number::text),
    congress,
    bill_type,
    number::text,
    title,
    origin_chamber,
    origin_chamber_code,
    introduced_date::text,
    policy_area,
    latest_action_date::text,
    latest_action_text,
    constitutional_authority_text,
    sponsor_member_id,
    text_url,
    text_format,
    text_version_type,
    text_date::text,
    text_content,
    text_embedding,
    summary_text,
    summary_action_desc,
    summary_action_date::text,
    update_date::text,
    update_date_including_text::text,
    legislation_url,
    api_url,
    created_at,
    updated_at,
    latest_text_version_id
  """

  override def upsert(bill: BillDO): F[Unit] =
    sql"""
      INSERT INTO $table (
        congress, bill_type, number, title,
        origin_chamber, origin_chamber_code,
        introduced_date, policy_area,
        latest_action_date, latest_action_text,
        constitutional_authority_text, sponsor_member_id,
        text_url, text_format, text_version_type, text_date,
        text_content, text_embedding,
        summary_text, summary_action_desc, summary_action_date,
        update_date, update_date_including_text,
        legislation_url, api_url
      ) VALUES (
        ${bill.congress}, ${bill.billType}, ${bill.number}::int, ${bill.title},
        ${bill.originChamber}, ${bill.originChamberCode},
        ${bill.introducedDate}::date, ${bill.policyArea},
        ${bill.latestActionDate}::date, ${bill.latestActionText},
        ${bill.constitutionalAuthorityText}, ${bill.sponsorMemberId},
        ${bill.textUrl}, ${bill.textFormat}, ${bill.textVersionType},
        ${bill.textDate}::timestamptz,
        ${bill.textContent}, ${bill.textEmbedding},
        ${bill.summaryText}, ${bill.summaryActionDesc},
        ${bill.summaryActionDate}::date,
        ${bill.updateDate}::timestamptz, ${bill.updateDateIncludingText}::timestamptz,
        ${bill.legislationUrl}, ${bill.apiUrl}
      )
      ON CONFLICT (congress, bill_type, number) DO UPDATE SET
        title = EXCLUDED.title,
        origin_chamber = EXCLUDED.origin_chamber,
        origin_chamber_code = EXCLUDED.origin_chamber_code,
        introduced_date = EXCLUDED.introduced_date,
        policy_area = EXCLUDED.policy_area,
        latest_action_date = EXCLUDED.latest_action_date,
        latest_action_text = EXCLUDED.latest_action_text,
        constitutional_authority_text = EXCLUDED.constitutional_authority_text,
        sponsor_member_id = EXCLUDED.sponsor_member_id,
        text_url = EXCLUDED.text_url,
        text_format = EXCLUDED.text_format,
        text_version_type = EXCLUDED.text_version_type,
        text_date = EXCLUDED.text_date,
        text_content = EXCLUDED.text_content,
        text_embedding = EXCLUDED.text_embedding,
        summary_text = EXCLUDED.summary_text,
        summary_action_desc = EXCLUDED.summary_action_desc,
        summary_action_date = EXCLUDED.summary_action_date,
        update_date = EXCLUDED.update_date,
        update_date_including_text = EXCLUDED.update_date_including_text,
        legislation_url = EXCLUDED.legislation_url,
        api_url = EXCLUDED.api_url,
        updated_at = NOW()
    """.update.run.transact(xa).void.adaptErr {
      case e =>
        BillUpsertFailed(bill.naturalKey, e.getMessage, e)
    }

  override def findByBillId(billId: String): F[Option[BillDO]] = {
    val (congress, billType, number) = parseNaturalKey(billId)
    (fr"SELECT" ++ selectColumns ++ fr"FROM $table WHERE congress = $congress AND bill_type = $billType AND number = $number::int")
      .query[BillDO]
      .option
      .transact(xa)
  }

  override def findByBillIds(billIds: List[String]): F[List[BillDO]] =
    if (billIds.isEmpty) {
      List.empty[BillDO].pure[F]
    } else {
      val parsed    = billIds.map(parseNaturalKey)
      val fragments = parsed.map { case (c, t, n) => fr"(congress = $c AND bill_type = $t AND number = $n::int)" }
      val conditions = fragments match {
        case first :: rest => rest.foldLeft(first)(_ ++ fr" OR " ++ _)
        case Nil           => fr"FALSE"
      }
      (fr"SELECT" ++ selectColumns ++ fr"FROM $table WHERE" ++ conditions)
        .query[BillDO]
        .to[List]
        .transact(xa)
    }

  override def findBillsNeedingTextCheck(): F[List[BillDO]] =
    (fr"SELECT" ++ selectColumns ++ fr"""FROM $table
      WHERE text_url IS NULL
         OR (text_version_type IS DISTINCT FROM 'ENR')""")
      .query[BillDO]
      .to[List]
      .transact(xa)

  override def updateTextFields(
    billId: String,
    textUrl: String,
    textFormat: String,
    textVersionType: String,
    textDate: String,
    latestTextVersionId: UUID,
  ): F[Unit] = {
    val (congress, billType, number) = parseNaturalKey(billId)
    sql"""
      UPDATE $table SET
        text_url = $textUrl,
        text_format = $textFormat,
        text_version_type = $textVersionType,
        text_date = $textDate::timestamptz,
        latest_text_version_id = $latestTextVersionId,
        updated_at = NOW()
      WHERE congress = $congress AND bill_type = $billType AND number = $number::int
    """.update.run.transact(xa).void
  }

  private[persistence] def parseNaturalKey(naturalKey: String): (Int, String, String) = {
    val parts = naturalKey.split("-", 3)
    (parts(0).toInt, parts(1).toLowerCase, parts(2))
  }

}
