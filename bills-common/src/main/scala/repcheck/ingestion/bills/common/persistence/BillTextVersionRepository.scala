package repcheck.ingestion.bills.common.persistence

import repcheck.shared.models.congress.dos.bill.BillTextVersionDO

trait BillTextVersionRepository[F[_]] {
  def insertVersion(version: BillTextVersionDO): F[Long]
  def findByBillId(billId: Long): F[List[BillTextVersionDO]]
  def findLatestByBillId(billId: Long): F[Option[BillTextVersionDO]]
  def storeAndUpdateBill(version: BillTextVersionDO): F[Long]
}
