package com.repcheck.bills.common.persistence

import repcheck.shared.models.congress.dos.bill.BillDO

trait BillRepository[F[_]] {
  def upsert(bill: BillDO): F[Long]
  def findByBillId(billId: String): F[Option[BillDO]]
  def findByBillIds(billIds: List[String]): F[List[BillDO]]
  def findBillsNeedingTextCheck(): F[List[BillDO]]

  def updateTextFields(
    billId: String,
    textUrl: String,
    textFormat: String,
    textVersionType: String,
    textDate: String,
    latestTextVersionId: Long,
  ): F[Unit]

}
