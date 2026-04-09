package com.repcheck.bills.common.persistence

import repcheck.shared.models.congress.dos.bill.BillCosponsorDO

trait BillCosponsorRepository[F[_]] {
  def replaceAll(billId: Long, cosponsors: List[BillCosponsorDO]): F[Unit]
  def findByBillId(billId: Long): F[List[BillCosponsorDO]]
}
