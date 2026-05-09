package repcheck.ingestion.amendments.text.persistence

import java.time.Instant

import repcheck.shared.models.congress.dos.amendment.AmendmentTextVersionDO

/**
 * Persistence boundary for `amendment_text_versions` (db-migrations 007/011/039). One row per amendment text version
 * (Submitted / Modified, in HTML or PDF). The actual extracted text + per-chunk embeddings live in
 * `amendment_text_chunks` rows joined by `version_id`.
 *
 * Mirrors [[repcheck.ingestion.bills.common.persistence.BillTextVersionRepository]] but keyed on amendment.
 *
 * The `upsert` method is the single-statement INSERT-ON-CONFLICT-DO-UPDATE writer used by the §7.6 processor — one DB
 * roundtrip folds the skip-check, re-submission detection, and orphan-recovery decision into the SQL itself per the
 * spec's Section 4 ("S4: Single-statement upsert").
 */
trait AmendmentTextVersionRepository[F[_]] {

  /**
   * Single-statement upsert: returns `(versionId, inserted, alreadyComplete)`.
   *
   *   - `versionId` — surrogate id of the row that's now in the table (whether just inserted or pre-existing).
   *   - `inserted` — true iff a new row was inserted (xmax = 0); false iff this was an UPDATE or NO-OP.
   *   - `alreadyComplete` — true iff the existing row was already complete (`fetched_at IS NOT NULL`) AND we did NOT
   *     trigger a refresh path. The processor short-circuits to Skipped when this is true.
   *
   * Re-submission semantics (per L6): the ON CONFLICT DO UPDATE clause has a `WHERE` that only fires the refresh when
   * the upstream `versionDate` is strictly newer than the stored `version_date`, OR when the existing row never
   * completed (`fetched_at IS NULL` — recovery from a previous crash). Stale re-deliveries with an older `versionDate`
   * short-circuit through `alreadyComplete = true` and the processor returns Skipped.
   */
  def upsert(version: AmendmentTextVersionDO): F[(Long, Boolean, Boolean)]

  /** Mark the supplied version row complete by setting `fetched_at = $timestamp` and `text_length = $textLength`. */
  def markFetched(versionId: Long, timestamp: Instant, textLength: Int): F[Unit]

  /** Read accessor used by analysis consumers (out of §7 scope but part of the repository contract). */
  def findCompletedByAmendmentId(amendmentId: Long): F[List[AmendmentTextVersionDO]]

}
