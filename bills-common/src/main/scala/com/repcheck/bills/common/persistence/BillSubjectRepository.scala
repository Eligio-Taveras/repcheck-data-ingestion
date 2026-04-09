package com.repcheck.bills.common.persistence

import repcheck.shared.models.congress.dos.bill.BillSubjectDO

trait BillSubjectRepository[F[_]] {
  def replaceAll(billId: Long, subjects: List[BillSubjectDO]): F[Unit]
  def findByBillId(billId: Long): F[List[BillSubjectDO]]
}
