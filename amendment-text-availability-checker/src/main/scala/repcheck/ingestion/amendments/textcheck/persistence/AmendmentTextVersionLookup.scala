package repcheck.ingestion.amendments.textcheck.persistence

import doobie._
import doobie.implicits._

import repcheck.pipeline.models.constants.Tables

/**
 * Minimal read-only lookup against the `amendment_text_versions` table — returns the (versionTypeCode, formatType)
 * tuples already ingested for an amendment, so
 * [[repcheck.ingestion.amendments.textcheck.selection.AmendmentTextVersionSelector]] can subtract them from the
 * upstream feed before emitting events.
 *
 * §7.5 explicitly does NOT depend on §7.6's `AmendmentTextVersionDO` type — that lives in the pipeline that owns the
 * write side. A tuple-based shape keeps this file independent of the not-yet-built §7.6 model and decoupled from any
 * future column changes that don't affect the diff key.
 *
 * The two columns are read positionally: `version_type` first (the existing nullable TEXT column being widened to the
 * `amendment_text_version_code_type` enum in db-migrations 0.1.34/0.1.35; serialized in either form here as a `String`)
 * and `format_type` second.
 */
trait AmendmentTextVersionLookup[F[_]] {
  def findExistingVersions(amendmentId: Long): F[List[(String, String)]]
}

class DoobieAmendmentTextVersionLookup extends AmendmentTextVersionLookup[ConnectionIO] {

  private val table: Fragment = Fragment.const(Tables.AmendmentTextVersions)

  override def findExistingVersions(amendmentId: Long): ConnectionIO[List[(String, String)]] =
    (fr"SELECT version_type, format_type FROM" ++ table ++
      fr"WHERE amendment_id = $amendmentId AND version_type IS NOT NULL AND format_type IS NOT NULL")
      .query[(String, String)]
      .to[List]

}
