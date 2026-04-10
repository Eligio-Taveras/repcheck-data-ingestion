package com.repcheck.bills.common.persistence

import doobie._
import doobie.implicits._

import repcheck.pipeline.models.constants.Tables
import repcheck.shared.models.congress.dos.bill.BillCosponsorDO

class DoobieBillCosponsorRepository extends BillCosponsorRepository[ConnectionIO] {

  private val table = Fragment.const(Tables.BillCosponsors)

  override def replaceAll(billId: Long, cosponsors: List[BillCosponsorDO]): ConnectionIO[Unit] = {
    val delete = sql"DELETE FROM $table WHERE bill_id = $billId".update.run

    val insert = Update[(Long, Long, Option[Boolean], Option[String])](
      s"INSERT INTO ${Tables.BillCosponsors} (bill_id, member_id, is_original_cosponsor, sponsorship_date) VALUES (?, ?, ?, ?::date)"
    )

    val rows = cosponsors.map(c => (c.billId, c.memberId, c.isOriginalCosponsor, c.sponsorshipDate))

    for {
      _ <- delete
      _ <- insert.updateMany(rows)
    } yield ()
  }

  override def findByBillId(billId: Long): ConnectionIO[List[BillCosponsorDO]] =
    sql"SELECT bill_id, member_id, is_original_cosponsor, sponsorship_date::text FROM $table WHERE bill_id = $billId"
      .query[BillCosponsorDO]
      .to[List]

}
