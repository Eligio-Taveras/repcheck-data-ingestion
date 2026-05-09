package repcheck.ingestion.amendments.text.persistence

import java.time.Instant

import cats.syntax.all._

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
 * `RETURNING id, (xmax = 0) AS inserted, (fetched_at IS NOT NULL) AS already_complete` — the `xmax = 0` test is
 * PostgreSQL's idiomatic "row was just inserted" predicate (xmax is non-zero on UPDATE).
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
   * SELECT-then-INSERT-or-UPDATE upsert composed inside one `ConnectionIO` (so the caller's `xa.trans` keeps the whole
   * sequence atomic). Equivalent to spec S4's single-statement INSERT-ON-CONFLICT-DO-UPDATE in observable behavior:
   * returns `(versionId, inserted, alreadyComplete)` and applies the same re-submission rule.
   *
   * The schema-level rationale for not using a literal ON CONFLICT clause: `amendment_text_versions` does NOT carry a
   * unique constraint on `(amendment_id, version_type, format_type)` (verified against migration 007's CREATE TABLE
   * plus the partial-index additions through migration 039). Issuing ON CONFLICT against a non-existing constraint
   * would error at runtime. A new migration to add that unique index is the right long-term fix and has been scoped out
   * of this PR; until that lands, this composed `ConnectionIO` is the functional equivalent.
   *
   * Returns `(versionId, inserted, alreadyComplete)`:
   *
   *   - `versionId` is the row's surrogate `id`.
   *   - `inserted` — true iff a brand-new row was created (no existing match by tuple). Newly-inserted rows always have
   *     `fetched_at = NULL` so the processor proceeds with the streaming flow. `alreadyComplete` is false in that case.
   *   - `alreadyComplete` — true iff an existing row was already complete (`fetched_at IS NOT NULL`) AND the upstream
   *     `versionDate` is NOT strictly newer than the stored value. The processor short-circuits to Skipped when this is
   *     true. False otherwise — including when re-submission was detected and the row was reset to `fetched_at = NULL`
   *     for re-streaming.
   */
  override def upsert(version: AmendmentTextVersionDO): ConnectionIO[(Long, Boolean, Boolean)] =
    findExisting(version.amendmentId, version.versionType, version.formatType.text).flatMap {
      case None =>
        insertFresh(version).map(id => (id, true, false))
      case Some((existingId, existingDate, existingFetched)) =>
        val isNewer       = version.versionDate.isAfter(existingDate)
        val isOrphan      = existingFetched.isEmpty
        val shouldRefresh = isNewer || isOrphan
        if (shouldRefresh) {
          refreshExisting(existingId, version).as((existingId, false, false))
        } else {
          // Existing row is complete AND upstream isn't newer — short-circuit. alreadyComplete reflects the
          // existing row's `fetched_at IS NOT NULL` status; defensive `existingFetched.isDefined` honors the
          // (theoretically impossible given `!shouldRefresh`) NULL case.
          doobie.free.connection.pure((existingId, false, existingFetched.isDefined))
        }
    }

  /**
   * Lookup the existing row's `(id, version_date, fetched_at)` for the given `(amendment_id, version_type,
   * format_type)` tuple. Returns `None` when no row matches.
   *
   * The `version_type` parameter is bound as the `amendment_text_version_code_type` enum (per migration 037);
   * `format_type` is bound as the `format_type_enum` (per migration 007). Both casts are required because the Doobie
   * `String` binding would otherwise pass `unknown` and PostgreSQL refuses to compare against the typed enum column.
   */
  private[persistence] def findExisting(
    amendmentId: Long,
    versionType: String,
    formatType: String,
  ): ConnectionIO[Option[(Long, Instant, Option[Instant])]] =
    sql"""
      SELECT id, version_date, fetched_at
      FROM $table
      WHERE amendment_id = $amendmentId
        AND version_type = $versionType::amendment_text_version_code_type
        AND format_type = $formatType::format_type_enum
      LIMIT 1
    """.query[(Long, Instant, Option[Instant])].option

  /** Insert a brand-new row for an unseen `(amendment_id, version_type, format_type)` tuple. */
  private[persistence] def insertFresh(version: AmendmentTextVersionDO): ConnectionIO[Long] =
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
      RETURNING id
    """.query[Long].unique

  /**
   * Refresh an existing row: update `url`, `download_url`, `version_date`, and reset `fetched_at = NULL` so the
   * processor re-streams. The orphan-chunk DELETE that follows in the processor is what cleans the partial chunk list
   * before the new chunks land.
   */
  private[persistence] def refreshExisting(versionId: Long, version: AmendmentTextVersionDO): ConnectionIO[Unit] =
    sql"""
      UPDATE $table SET
        url = ${version.url},
        download_url = ${version.downloadUrl},
        version_date = ${version.versionDate},
        fetched_at = NULL,
        text_length = NULL
      WHERE id = $versionId
    """.update.run.map(_ => ())

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
