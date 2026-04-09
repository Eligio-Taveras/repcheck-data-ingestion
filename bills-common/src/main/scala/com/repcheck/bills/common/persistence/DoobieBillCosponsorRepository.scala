package com.repcheck.bills.common.persistence

import cats.effect.kernel.MonadCancelThrow
import cats.syntax.all._

import doobie._
import doobie.implicits._

import repcheck.pipeline.models.constants.Tables
import repcheck.shared.models.congress.dos.bill.BillCosponsorDO

import com.repcheck.bills.common.errors.BillCosponsorReplaceFailed

class DoobieBillCosponsorRepository[F[_]: MonadCancelThrow](xa: Transactor[F]) extends BillCosponsorRepository[F] {

  private val table = Fragment.const(Tables.BillCosponsors)

  override def replaceAll(billId: Long, cosponsors: List[BillCosponsorDO]): F[Unit] = {
    val delete = sql"DELETE FROM $table WHERE bill_id = $billId".update.run

    val insert = Update[(Long, Long, Option[Boolean], Option[String])](
      s"INSERT INTO ${Tables.BillCosponsors} (bill_id, member_id, is_original_cosponsor, sponsorship_date) VALUES (?, ?, ?, ?::date)"
    )

    val rows = cosponsors.map(c => (c.billId, c.memberId, c.isOriginalCosponsor, c.sponsorshipDate))

    val program = for {
      _ <- delete
      _ <- insert.updateMany(rows)
    } yield ()

    program.transact(xa).adaptErr {
      case e =>
        BillCosponsorReplaceFailed(billId, e.getMessage, e)
    }
  }

  override def findByBillId(billId: Long): F[List[BillCosponsorDO]] =
    sql"SELECT bill_id, member_id, is_original_cosponsor, sponsorship_date::text FROM $table WHERE bill_id = $billId"
      .query[BillCosponsorDO]
      .to[List]
      .transact(xa)

}
