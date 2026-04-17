package repcheck.ingestion.bills.common.persistence

import cats.syntax.all._

import doobie._
import doobie.implicits._
import doobie.postgres.implicits._

import repcheck.pipeline.models.constants.Tables
import repcheck.shared.models.congress.dos.bill.BillCosponsorDO

class DoobieBillCosponsorRepository extends BillCosponsorRepository[ConnectionIO] {

  private val table = Fragment.const(Tables.BillCosponsors)

  override def replaceAll(billId: Long, cosponsors: List[BillCosponsorDO]): ConnectionIO[Unit] = {
    val delete = sql"DELETE FROM $table WHERE bill_id = $billId".update.run
    val rows   = cosponsors.map(c => (c.billId, c.memberId, c.isOriginalCosponsor, c.sponsorshipDate))
    delete *> runInsert(rows).void
  }

  private[persistence] def runInsert(
    rows: List[(Long, Long, Option[Boolean], Option[java.time.LocalDate])]
  ): ConnectionIO[Int] = {
    val insert = Update[(Long, Long, Option[Boolean], Option[java.time.LocalDate])](
      s"INSERT INTO ${Tables.BillCosponsors} (bill_id, member_id, is_original_cosponsor, sponsorship_date) VALUES (?, ?, ?, ?)"
    )
    insert.updateMany(rows)
  }

  override def findByBillId(billId: Long): ConnectionIO[List[BillCosponsorDO]] =
    sql"SELECT bill_id, member_id, is_original_cosponsor, sponsorship_date FROM $table WHERE bill_id = $billId"
      .query[BillCosponsorDO]
      .to[List]

}
