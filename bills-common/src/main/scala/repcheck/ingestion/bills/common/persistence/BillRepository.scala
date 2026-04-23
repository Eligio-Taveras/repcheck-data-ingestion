package repcheck.ingestion.bills.common.persistence

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

  /**
   * Insert a placeholder `bills` row keyed by the supplied natural key. Idempotent — repeated calls for the same
   * natural key are no-ops (`ON CONFLICT (congress, bill_type, number) DO NOTHING`). Used by consumers that discover a
   * reference to a bill they haven't ingested yet (e.g. the votes pipeline processing a bill-linked vote before
   * bills-pipeline has run) so the FK from `votes.bill_id` can resolve immediately; the downstream bills-pipeline
   * enriches the placeholder with real metadata on its next scheduled run.
   *
   * Raises `repcheck.ingestion.bills.common.errors.InvalidBillNaturalKey` if the natural key can't be parsed.
   *
   * @param naturalKey
   *   must match the `"<congress>-<BILL_TYPE>-<number>"` format produced by `BillConversions.buildBillNaturalKey` —
   *   e.g. `"119-HR-30"`.
   */
  def upsertPlaceholder(naturalKey: String): F[Unit]

}
