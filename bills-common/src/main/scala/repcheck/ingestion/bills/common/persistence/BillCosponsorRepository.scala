package repcheck.ingestion.bills.common.persistence

import repcheck.shared.models.congress.dos.bill.BillCosponsorDO

trait BillCosponsorRepository[F[_]] {

  /**
   * Atomically replaces all cosponsors for a bill (delete + batch insert in one transaction).
   *
   * Uses delete-and-replace rather than individual UPDATEs because cosponsors are a complete list from the API with no
   * stable secondary key beyond (bill_id, member_id) — diffing adds complexity without benefit when the API always
   * provides the full current set.
   */
  def replaceAll(billId: Long, cosponsors: List[BillCosponsorDO]): F[Unit]

  def findByBillId(billId: Long): F[List[BillCosponsorDO]]
}
