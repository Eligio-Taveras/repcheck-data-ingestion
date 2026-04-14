package com.repcheck.bills.common.persistence

import doobie._
import doobie.implicits._
import doobie.postgres.implicits._

import repcheck.pipeline.models.constants.Tables
import repcheck.shared.models.congress.common.DoobieEnumInstances._
import repcheck.shared.models.congress.dos.bill.BillTextVersionDO

import com.repcheck.bills.common.persistence.DoobieInstances.{floatArrayGet, floatArrayPut}

class DoobieBillTextVersionRepository extends BillTextVersionRepository[ConnectionIO] {

  private val table = Fragment.const(Tables.BillTextVersions)

  private val selectColumns: Fragment = fr"""
    id,
    bill_id,
    version_code,
    version_type,
    version_date::date,
    format_type,
    url,
    content,
    embedding,
    fetched_at,
    created_at
  """

  override def insertVersion(version: BillTextVersionDO): ConnectionIO[Long] =
    sql"""
      INSERT INTO $table (
        bill_id, version_code, version_type,
        version_date, format_type, url, content, embedding, fetched_at
      ) VALUES (
        ${version.billId}, ${version.versionCode}::text_version_code_type, ${version.versionType},
        ${version.versionDate}, ${version.formatType}, ${version.url},
        ${version.content}, ${version.embedding}::vector, ${version.fetchedAt}
      )
      ON CONFLICT (bill_id, version_code, version_date) DO UPDATE SET
        content = EXCLUDED.content,
        embedding = EXCLUDED.embedding,
        format_type = EXCLUDED.format_type,
        url = EXCLUDED.url,
        fetched_at = EXCLUDED.fetched_at
      RETURNING id
    """.query[Long].unique

  override def findByBillId(billId: Long): ConnectionIO[List[BillTextVersionDO]] =
    (fr"SELECT" ++ selectColumns ++ fr"FROM $table WHERE bill_id = $billId ORDER BY version_date DESC NULLS LAST")
      .query[BillTextVersionDO]
      .to[List]

  override def findLatestByBillId(billId: Long): ConnectionIO[Option[BillTextVersionDO]] =
    (fr"SELECT" ++ selectColumns ++ fr"FROM $table WHERE bill_id = $billId ORDER BY version_date DESC NULLS LAST LIMIT 1")
      .query[BillTextVersionDO]
      .option

  private[persistence] def buildBillTextFieldsUpdate(
    version: BillTextVersionDO,
    generatedId: Long,
  ): ConnectionIO[Int] = {
    val billsTable = Fragment.const(Tables.Bills)
    sql"""
      UPDATE $billsTable SET
        text_url = ${version.url},
        text_format = ${version.formatType},
        text_version_type = ${version.versionCode}::text_version_code_type,
        text_date = ${version.versionDate},
        latest_text_version_id = $generatedId,
        updated_at = NOW()
      WHERE id = ${version.billId}
    """.update.run
  }

  override def storeAndUpdateBill(version: BillTextVersionDO): ConnectionIO[Long] =
    for {
      generatedId <- sql"""
        INSERT INTO $table (
          bill_id, version_code, version_type,
          version_date, format_type, url, content, embedding, fetched_at
        ) VALUES (
          ${version.billId}, ${version.versionCode}::text_version_code_type, ${version.versionType},
          ${version.versionDate}, ${version.formatType}, ${version.url},
          ${version.content}, ${version.embedding}::vector, ${version.fetchedAt}
        )
        ON CONFLICT (bill_id, version_code, version_date) DO UPDATE SET
          content = EXCLUDED.content,
          embedding = EXCLUDED.embedding,
          format_type = EXCLUDED.format_type,
          url = EXCLUDED.url,
          fetched_at = EXCLUDED.fetched_at
        RETURNING id
      """.query[Long].unique

      _ <- buildBillTextFieldsUpdate(version, generatedId)
    } yield generatedId

}
