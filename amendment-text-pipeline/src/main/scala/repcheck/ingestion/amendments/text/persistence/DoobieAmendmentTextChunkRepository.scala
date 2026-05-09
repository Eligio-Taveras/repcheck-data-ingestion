package repcheck.ingestion.amendments.text.persistence

import doobie._
import doobie.implicits._
import doobie.postgres.implicits._

import repcheck.ingestion.bills.common.persistence.DoobieInstances.{floatArrayGet, floatArrayPut}
import repcheck.pipeline.models.constants.Tables
import repcheck.shared.models.congress.dos.amendment.AmendmentTextChunkDO

/**
 * Doobie implementation of [[AmendmentTextChunkRepository]] keyed at `ConnectionIO`.
 *
 * Mirrors [[repcheck.ingestion.bills.text.persistence.DoobieRawBillTextRepository]] byte-for-byte except for the table
 * name (`amendment_text_chunks` from `Tables.AmendmentTextChunks`) and the column shape — the amendment side has an
 * explicit `amendment_id` column (per migration 040) in addition to the optional `version_id`, matching the schema
 * decision to keep chunks reachable by amendment even during the brief window before a version row exists.
 *
 * The pgvector `embedding` column is written via the `?::vector` cast on the parameter — Doobie's `floatArrayPut`
 * formats `Array[Float]` as `[1.0,2.0,...]` and PostgreSQL rejects the literal without the explicit cast. Reads use
 * `floatArrayGet` which parses the same shape back into `Array[Float]`. Both reused from
 * `bills-common.DoobieInstances`.
 */
class DoobieAmendmentTextChunkRepository extends AmendmentTextChunkRepository[ConnectionIO] {

  private val tableName: String = Tables.AmendmentTextChunks
  private val table: Fragment   = Fragment.const(tableName)

  // Explicit column list — never SELECT *. Order matches the AmendmentTextChunkDO constructor field order.
  private val selectColumns: Fragment = fr"""
    id,
    amendment_id,
    version_id,
    chunk_index,
    content,
    embedding,
    created_at
  """

  // Update[] needs a literal SQL string (not a Fragment) so the table-name constant is inlined here.
  private val insertSql: String =
    s"INSERT INTO $tableName (amendment_id, version_id, chunk_index, content, embedding) VALUES (?, ?, ?, ?, ?::vector)"

  override def deleteByVersionId(versionId: Long): ConnectionIO[Unit] =
    sql"DELETE FROM $table WHERE version_id = $versionId".update.run.map(_ => ())

  override def insertMany(rows: List[AmendmentTextChunkDO]): ConnectionIO[Unit] =
    if (rows.isEmpty) {
      doobie.free.connection.unit
    } else {
      val params: List[(Long, Option[Long], Int, String, Option[Array[Float]])] =
        rows.map(r => (r.amendmentId, r.versionId, r.chunkIndex, r.content, r.embedding))
      Update[(Long, Option[Long], Int, String, Option[Array[Float]])](insertSql)
        .updateMany(params)
        .map(_ => ())
    }

  override def findByVersionId(versionId: Long): ConnectionIO[List[AmendmentTextChunkDO]] =
    (fr"SELECT" ++ selectColumns ++
      fr"FROM $table WHERE version_id = $versionId ORDER BY chunk_index ASC")
      .query[AmendmentTextChunkDO]
      .to[List]

  override def countByVersionId(versionId: Long): ConnectionIO[Long] =
    sql"SELECT COUNT(*) FROM $table WHERE version_id = $versionId"
      .query[Long]
      .unique

  override def sumContentLengthByVersionId(versionId: Long): ConnectionIO[Long] =
    sql"SELECT COALESCE(SUM(LENGTH(content)), 0) FROM $table WHERE version_id = $versionId"
      .query[Long]
      .unique

}
