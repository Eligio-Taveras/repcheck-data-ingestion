package com.repcheck.bills.common.persistence

import java.util.UUID

import repcheck.shared.models.congress.dos.bill.BillTextVersionDO

trait BillTextVersionRepository[F[_]] {
  def insertVersion(version: BillTextVersionDO): F[UUID]
  def findByBillId(billId: Long): F[List[BillTextVersionDO]]
  def findLatestByBillId(billId: Long): F[Option[BillTextVersionDO]]
  def storeAndUpdateBill(version: BillTextVersionDO): F[UUID]
}
