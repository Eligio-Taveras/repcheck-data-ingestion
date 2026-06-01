package repcheck.ingestion.amendments.persistence

import doobie._
import doobie.implicits._

import repcheck.pipeline.models.constants.Tables

/**
 * Doobie implementation of [[CommitteeLookupRepository]] for `ConnectionIO`. Matches the sponsor's `systemCode` against
 * the path segment of `committees.url` via a POSIX `substring(... from ...)` capture. systemCodes are globally unique
 * across chambers (the `hs`/`ss`/`jj` prefix encodes the chamber), so the match yields at most one row; `LIMIT 1` is a
 * belt-and-braces guard against a malformed duplicate URL.
 */
class DoobieCommitteeLookupRepository extends CommitteeLookupRepository[ConnectionIO] {

  private val table: Fragment = Fragment.const(Tables.Committees)

  override def findIdBySystemCode(systemCode: String): ConnectionIO[Option[Long]] =
    sql"""SELECT id FROM $table
          WHERE substring(url FROM 'committee/[a-z]+/([a-z0-9]+)') = ${systemCode.toLowerCase}
          LIMIT 1""".query[Long].option

}
