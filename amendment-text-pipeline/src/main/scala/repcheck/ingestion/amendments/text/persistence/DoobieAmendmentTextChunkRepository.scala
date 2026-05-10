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
 * Mirrors [[repcheck.ingestion.bills.text.persistence.DoobieRawBillTextRepository]] for the amendment side, with
 * different table + column names (`amendment_text_chunks` from `Tables.AmendmentTextChunks`) and an explicit
 * `amendment_id` column (per migration 040) in addition to `version_id`.
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

  // Update[] needs a literal SQL string (not a Fragment) so the table-name constant is inlined here. Idempotent
  // last-writer-wins on `(version_id, chunk_index)` — re-deliveries overwrite content + embedding in place. No
  // version-date gate because the natural identity (amendment_id, version_type, format_type) → versionId is stable
  // at Congress.gov and the chunker is deterministic; same id → same chunks.
  private val upsertSql: String =
    s"""INSERT INTO $tableName (amendment_id, version_id, chunk_index, content, embedding)
       |VALUES (?, ?, ?, ?, ?::vector)
       |ON CONFLICT (version_id, chunk_index) WHERE version_id IS NOT NULL DO UPDATE SET
       |  content   = EXCLUDED.content,
       |  embedding = EXCLUDED.embedding""".stripMargin

  override def upsertMany(rows: List[AmendmentChunkRow]): ConnectionIO[Int] =
    if (rows.isEmpty) {
      doobie.free.connection.pure(0)
    } else {
      val params: List[(Long, Long, Int, String, Option[Array[Float]])] =
        rows.map(r => (r.amendmentId, r.versionId, r.chunkIndex, r.content, r.embedding))
      Update[(Long, Long, Int, String, Option[Array[Float]])](upsertSql)
        .updateMany(params)
    }

  override def trimChunksPast(versionId: Long, chunkCount: Int): ConnectionIO[Int] =
    sql"DELETE FROM $table WHERE version_id = $versionId AND chunk_index >= $chunkCount".update.run

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
