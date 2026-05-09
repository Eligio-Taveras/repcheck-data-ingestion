package repcheck.ingestion.amendments.text.persistence

import java.time.Instant

import doobie._
import doobie.implicits._
import doobie.postgres.implicits._

import repcheck.pipeline.models.constants.Tables
import repcheck.shared.models.congress.common.DoobieEnumInstances._
import repcheck.shared.models.congress.dos.amendment.AmendmentTextVersionDO

/**
 * Doobie implementation of [[AmendmentTextVersionRepository]] keyed at `ConnectionIO`.
 *
 * The single-statement [[upsert]] folds the skip-check, the re-submission detection, and the orphan-recovery branch
 * into one SQL roundtrip per §7.6 spec Section 4 (S4). Returns a `(versionId, inserted, alreadyComplete)` triple via
 * `RETURNING id, (xmax = 0) AS inserted, (fetched_at IS NOT NULL) AS already_complete` — `xmax = 0` is PostgreSQL's
 * idiomatic "row was just inserted" predicate (xmax is non-zero on UPDATE), and the post-UPDATE `fetched_at` value
 * tells the processor whether the existing row was already complete and therefore safe to short-circuit.
 *
 * The ON CONFLICT clause keys on `(amendment_id, version_type, format_type)` — the unique constraint added in
 * db-migrations 0.1.36 makes this the canonical writer form.
 *
 * [[markFetched]] sets `fetched_at` and `text_length` together so the completion-marker UPDATE is one statement; the
 * bill-side has the same shape.
 */
class DoobieAmendmentTextVersionRepository extends AmendmentTextVersionRepository[ConnectionIO] {

  private val table: Fragment = Fragment.const(Tables.AmendmentTextVersions)

  // Explicit column list — never SELECT *. Order matches the AmendmentTextVersionDO constructor.
  private val selectColumns: Fragment = fr"""
    id,
    amendment_id,
    version_type,
    version_date,
    format_type,
    url,
    download_url,
    text_length,
    fetched_at,
    created_at
  """

  /**
   * Single-statement INSERT ... ON CONFLICT DO UPDATE. Keys on the unique tuple `(amendment_id, version_type,
   * format_type)` introduced in db-migrations 0.1.36.
   *
   * Returns `(versionId, inserted, alreadyComplete)`:
   *
   *   - `versionId` is the row's surrogate `id`.
   *   - `inserted` (from `xmax = 0`) — true iff a brand-new row was created. Newly-inserted rows always have
   *     `fetched_at = NULL` so `alreadyComplete` is false in that case.
   *   - `alreadyComplete` (from `fetched_at IS NOT NULL`) — true iff after the upsert the row's `fetched_at` is set,
   *     which only happens when the existing row was already complete AND the upstream `versionDate` was NOT strictly
   *     newer (the CASE expressions in the SET clause leave `fetched_at`/`text_length` untouched in that case). The
   *     processor short-circuits to `Skipped` when this is true. False otherwise — including the re-submission-with-
   *     newer-date path, where the CASE expressions reset both columns to NULL so the processor re-streams.
   */
  override def upsert(version: AmendmentTextVersionDO): ConnectionIO[(Long, Boolean, Boolean)] =
    sql"""
      INSERT INTO $table (
        amendment_id, version_type, version_date, format_type, url, download_url, text_length, fetched_at
      ) VALUES (
        ${version.amendmentId},
        ${version.versionType}::amendment_text_version_code_type,
        ${version.versionDate},
        ${version.formatType},
        ${version.url},
        ${version.downloadUrl},
        ${version.textLength},
        ${version.fetchedAt}
      )
      ON CONFLICT (amendment_id, version_type, format_type) DO UPDATE SET
        version_date = EXCLUDED.version_date,
        url = EXCLUDED.url,
        download_url = EXCLUDED.download_url,
        fetched_at = CASE
          WHEN EXCLUDED.version_date > amendment_text_versions.version_date THEN NULL
          ELSE amendment_text_versions.fetched_at
        END,
        text_length = CASE
          WHEN EXCLUDED.version_date > amendment_text_versions.version_date THEN NULL
          ELSE amendment_text_versions.text_length
        END
      RETURNING id, (xmax = 0) AS inserted, (fetched_at IS NOT NULL) AS already_complete
    """.query[(Long, Boolean, Boolean)].unique

  override def markFetched(versionId: Long, timestamp: Instant, textLength: Int): ConnectionIO[Unit] =
    sql"""
      UPDATE $table SET fetched_at = $timestamp, text_length = $textLength WHERE id = $versionId
    """.update.run.map(_ => ())

  override def findCompletedByAmendmentId(amendmentId: Long): ConnectionIO[List[AmendmentTextVersionDO]] =
    (fr"SELECT" ++ selectColumns ++
      fr"FROM $table WHERE amendment_id = $amendmentId AND fetched_at IS NOT NULL ORDER BY version_date DESC")
      .query[AmendmentTextVersionDO]
      .to[List]

}
