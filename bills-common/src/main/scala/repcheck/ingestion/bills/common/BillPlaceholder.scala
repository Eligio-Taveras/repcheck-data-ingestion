package repcheck.ingestion.bills.common

import repcheck.shared.models.congress.dos.bill.BillDO

/**
 * Canonical placeholder detection for `BillDO` rows, scoped to the bill-metadata-pipeline's ownership.
 *
 * A "placeholder bill" is a row inserted by an upstream pipeline (currently bill-text-availability-checker and
 * bill-summary-pipeline; potentially votes-pipeline / amendments-pipeline if they reference bills not yet ingested)
 * that has the natural-key fields populated but the metadata-owned fields empty. The metadata pipeline backfills these
 * rows when it sees them on its sweep.
 *
 * Only metadata-pipeline-owned fields are checked here. `text_*` columns belong to bill-text-pipeline and `summary_*`
 * columns belong to bill-summary-pipeline; including them in the check used to mis-classify fully-hydrated rows as
 * placeholders (those columns are `None` until the downstream pipelines populate them, even for bills the metadata
 * sweep has finished). The result was that the "Bill unchanged" skip branch never fired and every sweep re-ran the
 * detail fetch for every row.
 *
 * The four fields below are the canonical metadata-pipeline outputs:
 *   - `sponsor_member_id` — the bill's resolved sponsor (placeholder or real)
 *   - `introduced_date` — the introduction date from the API detail call
 *   - `latest_action_text` — the latest legislative action's text
 *   - `update_date` — the API's reported lastModified for the bill
 *
 * If any of these is `None` the row hasn't been through a successful metadata backfill yet and we should re-fetch. If
 * all four are populated, the row was hydrated by this pipeline and the caller should fall through to the `update_date`
 * comparison for the unchanged-skip path.
 */
object BillPlaceholder {

  /**
   * Returns `true` iff the given bill is missing any of the metadata-owned fields and therefore needs a (re-)fetch.
   *
   * The metadata pipeline routes these through the update path regardless of any `update_date` comparison so the detail
   * fetch fills them in. Without this, a placeholder created by another pipeline (with `update_date` either `NULL` or
   * set to its own insertion timestamp) would either dodge the date comparison entirely or land on the "Bill unchanged"
   * branch forever.
   */
  def isPlaceholder(bill: BillDO): Boolean =
    bill.sponsorMemberId.isEmpty ||
      bill.introducedDate.isEmpty ||
      bill.latestActionText.isEmpty ||
      bill.updateDate.isEmpty

}
