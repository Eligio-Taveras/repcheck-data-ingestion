package repcheck.ingestion.bills.common.persistence

import java.time.Instant

import repcheck.shared.models.congress.dos.bill.BillTextVersionDO

trait BillTextVersionRepository[F[_]] {
  def insertVersion(version: BillTextVersionDO): F[Long]
  def findByBillId(billId: Long): F[List[BillTextVersionDO]]
  def findLatestByBillId(billId: Long): F[Option[BillTextVersionDO]]
  def storeAndUpdateBill(version: BillTextVersionDO): F[Long]

  /**
   * Mark the supplied version row as fully fetched by setting `fetched_at = $timestamp`. Used by the streaming-INSERT
   * flow as the completion signal: the version row is inserted with `fetched_at = NULL` while chunks are being
   * persisted, then this method flips it on after the last chunk lands. The skip-check in
   * [[repcheck.ingestion.bills.text.pipeline.BillTextProcessor.isAlreadyProcessed]] uses `fetched_at IS NOT NULL` to
   * tell complete-runs apart from partially-completed-then-crashed runs that still need re-processing.
   */
  def markFetched(versionId: Long, timestamp: Instant): F[Unit]
}
