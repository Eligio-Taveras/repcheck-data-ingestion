package com.repcheck.bills.common.persistence

import repcheck.shared.models.congress.dos.bill.BillSubjectDO

trait BillSubjectRepository[F[_]] {

  /**
   * Atomically replaces all subjects for a bill (delete + batch insert in one transaction).
   *
   * Uses delete-and-replace rather than individual UPDATEs because subjects are a complete list from the API with no
   * stable secondary key beyond (bill_id, subject_name) — diffing adds complexity without benefit when the API always
   * provides the full current set.
   */
  def replaceAll(billId: Long, subjects: List[BillSubjectDO]): F[Unit]

  def findByBillId(billId: Long): F[List[BillSubjectDO]]
}
