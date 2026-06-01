package repcheck.ingestion.bills.common.persistence

import repcheck.shared.models.congress.bill.TextVersionCode
import repcheck.shared.models.congress.dos.bill.BillDO

trait BillRepository[F[_]] {
  def upsert(bill: BillDO): F[Long]
  def findByBillId(billId: String): F[Option[BillDO]]
  def findByBillIds(billIds: List[String]): F[List[BillDO]]

  /**
   * Return bills whose stored `text_version_type` differs from `expected_text_version_code`, filtered to `congresses`
   * if non-empty (empty list = no congress filter).
   *
   * The congress filter exists because pre-103 bills get `expected_text_version_code` populated by the metadata
   * pipeline's chamber-floor write, but Congress.gov has no text body for them — so including them in the sweep just
   * burns API rate budget on guaranteed misses. Operators set the list (default 103-119) via
   * `BILL_TEXT_CHECK_CONGRESSES` env or `pipeline.congresses` HOCON.
   */
  def findBillsNeedingTextCheck(congresses: List[Int]): F[List[BillDO]]

  /**
   * Link the bill to the bill_text_versions row for its current text stage and keep `expected_text_version_code` in
   * sync. Post-Phase-2c the text body/url/format/date and version code live in bill_text_versions; bills only stores
   * `latest_text_version_id`. `textVersionType` is the downloaded stage (used for the cooperative expected-stage bump),
   * not written to bills.
   */
  def updateTextFields(
    billId: String,
    textVersionType: String,
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

  /**
   * Read the `expected_text_version_code` column for a single bill. Used by both `bill-metadata-pipeline` (to decide
   * whether to advance the introduced floor) and `bill-summary-pipeline` (to decide whether to advance from a CRS
   * summary's versionCode). The cooperative-write contract uses [[TextVersionCode.progressionOrder]] to gate updates —
   * see [[updateExpectedVersion]] — so the caller compares against the returned value.
   *
   * @return
   *   `None` if the bill doesn't exist or the column is NULL.
   */
  def findExpectedVersion(naturalKey: String): F[Option[TextVersionCode]]

  /**
   * Set `expected_text_version_code` on a single bill. The repository writes unconditionally — the `progressionOrder`
   * regression guard lives at the caller (bill-metadata-pipeline floor or bill-summary-pipeline advance) so the caller
   * decides whether to invoke this method based on `findExpectedVersion`'s return value. Keeps the SQL simple (one
   * UPDATE) and the cooperation logic explicit in Scala.
   *
   * Bumps `updated_at = NOW()` so downstream readers can see the row was touched.
   */
  def updateExpectedVersion(naturalKey: String, code: TextVersionCode): F[Unit]

}
