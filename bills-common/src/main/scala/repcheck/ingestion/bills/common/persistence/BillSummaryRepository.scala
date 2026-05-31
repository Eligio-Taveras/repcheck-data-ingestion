package repcheck.ingestion.bills.common.persistence

import java.time.LocalDate

import repcheck.shared.models.congress.bill.TextVersionCode

/**
 * Persistence boundary for `bill_summaries` (db-migrations 049). One row per CRS summary stage, keyed on `(bill_id,
 * version_code)`, owned by bill-summary-pipeline. Replaces the single `bills.summary_text` column (which held only one
 * stage and was clobbered by the bill-metadata upsert).
 */
trait BillSummaryRepository[F[_]] {

  /**
   * Upsert one stage's summary for the bill identified by `naturalKey` (`{congress}-{TYPE}-{number}`). Resolves
   * `bill_id` from `bills` in the same statement, so the caller only needs to have ensured the bill exists
   * (placeholder). Idempotent on `(bill_id, version_code)`: a later fetch of the same stage refreshes text/desc/date.
   */
  def upsert(
    naturalKey: String,
    versionCode: TextVersionCode,
    text: String,
    actionDesc: Option[String],
    actionDate: Option[LocalDate],
  ): F[Unit]

}
