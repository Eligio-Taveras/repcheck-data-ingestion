package repcheck.ingestion.bills.common

import repcheck.shared.models.congress.dos.bill.BillDO

/**
 * Canonical placeholder detection for `BillDO` rows.
 *
 * A "placeholder bill" is a row inserted by an upstream pipeline (currently bill-text-availability-checker and
 * bill-summary-pipeline; potentially votes-pipeline / amendments-pipeline if they reference bills not yet ingested)
 * that has the natural-key fields populated but every detail field empty. The metadata pipeline backfills these rows
 * when it sees them on its sweep.
 *
 * The shape this object detects MUST match the shape produced by `repcheck.shared.models.congress.dos.bill.BillDO`'s
 * `HasPlaceholder[BillDO].placeholder(naturalKey)` factory in shared-models. The unit test in `BillPlaceholderSpec`
 * pins that invariant — adding a new detail field to `BillDO` will fail the test until both sides are updated.
 *
 * The check on `title.isEmpty` plus all `Option` detail fields being `None` is the canonical definition. We don't check
 * the natural-key fields (`congress`, `billType`, `number`) because the writer pipelines override the placeholder()
 * defaults with the real natural-key values to satisfy the unique constraint on `(congress, bill_type, number)`.
 */
object BillPlaceholder {

  /**
   * Returns `true` iff the given bill is a placeholder row — title is empty AND every detail Option is `None`.
   *
   * The metadata pipeline routes these through the update path regardless of any `update_date` comparison so the detail
   * fetch fills them in. Without this, the placeholder's stored `update_date` (set to the writer's insertion time, by
   * definition newer than the API's real updateDate for older bills) would land them on the "Bill unchanged" branch
   * forever.
   */
  def isPlaceholder(bill: BillDO): Boolean =
    bill.title.isEmpty &&
      bill.originChamber.isEmpty &&
      bill.originChamberCode.isEmpty &&
      bill.introducedDate.isEmpty &&
      bill.policyArea.isEmpty &&
      bill.latestActionDate.isEmpty &&
      bill.latestActionText.isEmpty &&
      bill.constitutionalAuthorityText.isEmpty &&
      bill.sponsorMemberId.isEmpty &&
      bill.textUrl.isEmpty &&
      bill.textFormat.isEmpty &&
      bill.textVersionType.isEmpty &&
      bill.textDate.isEmpty &&
      bill.textContent.isEmpty &&
      bill.summaryText.isEmpty &&
      bill.summaryActionDesc.isEmpty &&
      bill.summaryActionDate.isEmpty &&
      bill.legislationUrl.isEmpty &&
      bill.apiUrl.isEmpty

}
