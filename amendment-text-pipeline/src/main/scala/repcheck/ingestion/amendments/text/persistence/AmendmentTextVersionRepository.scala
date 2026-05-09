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
   *   - `inserted` — true iff a new row was inserted (xmax = 0); false iff this was an UPDATE.
   *   - `alreadyComplete` — true iff after the upsert the row's `fetched_at IS NOT NULL`. Implies either (a) the row
   *     was already complete AND the upstream wasn't strictly newer (so CASE left it untouched), or (b) something else
   *     in this family of paths — never true on inserts. The processor short-circuits to `Skipped` when this is true.
   *
   * Re-submission semantics (per L6): the implementation uses CASE expressions over every mutable column gated on
   * `EXCLUDED.version_date > stored.version_date`. When the upstream `versionDate` is strictly newer, all of
   * `version_date`, `url`, `download_url` are refreshed and `fetched_at`/`text_length` are reset to NULL so the
   * processor re-streams. Otherwise — stale or duplicate redelivery — the CASE expressions select the stored values
   * unchanged, the row reports `alreadyComplete = true` if it was complete, and the processor returns Skipped.
   *
   * Why CASE rather than a `WHERE` on the UPDATE: a top-level `WHERE` on `ON CONFLICT DO UPDATE` causes Postgres to
   * skip the UPDATE on stale redeliveries and emit no row in `RETURNING`, which would break `.unique` decoding. CASE
   * always emits the row (with stored values flowing through on the no-op branch), giving the processor a clean
   * `(versionId, inserted, alreadyComplete)` triple for every event regardless of staleness.
   */
  def upsert(version: AmendmentTextVersionDO): F[(Long, Boolean, Boolean)]

  /** Mark the supplied version row complete by setting `fetched_at = $timestamp` and `text_length = $textLength`. */
  def markFetched(versionId: Long, timestamp: Instant, textLength: Int): F[Unit]

  /** Read accessor used by analysis consumers (out of §7 scope but part of the repository contract). */
  def findCompletedByAmendmentId(amendmentId: Long): F[List[AmendmentTextVersionDO]]

}
