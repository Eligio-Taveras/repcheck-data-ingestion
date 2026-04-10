package com.repcheck.bills.common.persistence

trait BillHistoryArchiver[F[_]] {
  def archiveBill(billId: String): F[Long]
}
