package com.repcheck.bills.common.persistence

import cats.effect.kernel.MonadCancelThrow
import cats.syntax.all._

import doobie._
import doobie.implicits._
import doobie.postgres.implicits._

import repcheck.pipeline.models.constants.Tables
import repcheck.shared.models.congress.dos.bill.BillTextVersionDO

import com.repcheck.bills.common.errors.BillTextVersionInsertFailed
import com.repcheck.bills.common.persistence.DoobieInstances.{floatArrayGet, floatArrayPut}

class DoobieBillTextVersionRepository[F[_]: MonadCancelThrow](xa: Transactor[F]) extends BillTextVersionRepository[F] {

  private val table = Fragment.const(Tables.BillTextVersions)

  private val selectColumns: Fragment = fr"""
    id,
    bill_id,
    version_code,
    version_type,
    version_date::text,
    format_type,
    url,
    content,
    embedding,
    fetched_at,
    created_at
  """

  override def insertVersion(version: BillTextVersionDO): F[Long] =
    sql"""
      INSERT INTO $table (
        bill_id, version_code, version_type,
        version_date, format_type, url, content, embedding, fetched_at
      ) VALUES (
        ${version.billId}, ${version.versionCode}, ${version.versionType},
        ${version.versionDate}::timestamptz, ${version.formatType}, ${version.url},
        ${version.content}, ${version.embedding}::vector, ${version.fetchedAt}
      )
      RETURNING id
    """.query[Long].unique.transact(xa).adaptErr {
      case e =>
        BillTextVersionInsertFailed(version.billId, e.getMessage, e)
    }

  override def findByBillId(billId: Long): F[List[BillTextVersionDO]] =
    (fr"SELECT" ++ selectColumns ++ fr"FROM $table WHERE bill_id = $billId ORDER BY version_date DESC NULLS LAST")
      .query[BillTextVersionDO]
      .to[List]
      .transact(xa)

  override def findLatestByBillId(billId: Long): F[Option[BillTextVersionDO]] =
    (fr"SELECT" ++ selectColumns ++ fr"FROM $table WHERE bill_id = $billId ORDER BY version_date DESC NULLS LAST LIMIT 1")
      .query[BillTextVersionDO]
      .option
      .transact(xa)

  override def storeAndUpdateBill(version: BillTextVersionDO): F[Long] = {
    val billsTable = Fragment.const(Tables.Bills)

    val program = for {
      generatedId <- sql"""
        INSERT INTO $table (
          bill_id, version_code, version_type,
          version_date, format_type, url, content, embedding, fetched_at
        ) VALUES (
          ${version.billId}, ${version.versionCode}, ${version.versionType},
          ${version.versionDate}::timestamptz, ${version.formatType}, ${version.url},
          ${version.content}, ${version.embedding}::vector, ${version.fetchedAt}
        )
        RETURNING id
      """.query[Long].unique

      _ <- sql"""
        UPDATE $billsTable SET
          text_url = ${version.url},
          text_format = ${version.formatType},
          text_version_type = ${version.versionCode},
          text_date = ${version.versionDate}::timestamptz,
          latest_text_version_id = $generatedId,
          updated_at = NOW()
        WHERE id = ${version.billId}
      """.update.run
    } yield generatedId

    program.transact(xa).adaptErr {
      case e =>
        BillTextVersionInsertFailed(version.billId, e.getMessage, e)
    }
  }

}
