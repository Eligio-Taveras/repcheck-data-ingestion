package com.repcheck.bills.common.persistence

import java.util.UUID

trait BillHistoryArchiver[F[_]] {
  def archiveBill(billId: String): F[UUID]
}
