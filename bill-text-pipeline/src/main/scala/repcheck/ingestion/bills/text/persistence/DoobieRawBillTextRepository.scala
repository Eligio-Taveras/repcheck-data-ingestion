package repcheck.ingestion.bills.text.persistence

import doobie._
import doobie.implicits._
import doobie.postgres.implicits._

import repcheck.ingestion.bills.common.persistence.DoobieInstances.{floatArrayGet, floatArrayPut}
import repcheck.shared.models.congress.dos.bill.RawBillTextDO

/**
 * Doobie implementation of [[RawBillTextRepository]] keyed at `ConnectionIO`.
 *
 * `upsertMany` uses `INSERT ... ON CONFLICT (version_id, chunk_index) DO UPDATE` so concurrent / replayed deliveries
 * are last-writer-wins. The pgvector `embedding` column is written via the `::vector` cast on the parameter — Doobie's
 * `floatArrayPut` from `bills-common.DoobieInstances` formats `Array[Float]` as `[1.0,2.0,...]` and PostgreSQL rejects
 * the literal without the explicit cast. Reads use `floatArrayGet` which parses the same shape back into
 * `Array[Float]`.
 *
 * NOTE: the `raw_bill_text` table name is a literal here pending a `Tables.RawBillText` constant in
 * `repcheck-pipeline-models` (follow-up PR; deliberately deferred to keep this PR self-contained).
 */
class DoobieRawBillTextRepository extends RawBillTextRepository[ConnectionIO] {

  // TODO(pipeline-models): promote to Tables.RawBillText once a pipeline-models release lands.
  private val tableName: String = "raw_bill_text"
  private val table: Fragment   = Fragment.const(tableName)

  private val selectColumns: Fragment = fr"""
    id,
    bill_id,
    version_id,
    chunk_index,
    content,
    embedding,
    created_at
  """

  // Update[] needs a literal SQL string (not a Fragment) so the table name stays inlined here.
  // ON CONFLICT keys on (version_id, chunk_index) — the unique constraint defined in db-migrations 026.
  // EXCLUDED holds the row that would have been INSERTed; we copy ALL non-key columns including bill_id so a
  // corrected/replayed write can repair an earlier bad bill_id and keep the redundant key columns consistent.
  // Per the LWW semantics on (version_id, chunk_index), the latest writer wins for every non-key column.
  private val upsertSql: String =
    s"""INSERT INTO $tableName (bill_id, version_id, chunk_index, content, embedding)
       |VALUES (?, ?, ?, ?, ?::vector)
       |ON CONFLICT (version_id, chunk_index) DO UPDATE SET
       |  bill_id = EXCLUDED.bill_id,
       |  content = EXCLUDED.content,
       |  embedding = EXCLUDED.embedding""".stripMargin

  override def upsertMany(rows: List[RawBillTextDO]): ConnectionIO[Int] =
    if (rows.isEmpty) {
      doobie.free.connection.pure(0)
    } else {
      val params: List[(Long, Option[Long], Int, String, Option[Array[Float]])] =
        rows.map(r => (r.billId, r.versionId, r.chunkIndex, r.content, r.embedding))
      Update[(Long, Option[Long], Int, String, Option[Array[Float]])](upsertSql).updateMany(params)
    }

  override def trimChunksPast(versionId: Long, chunkCount: Int): ConnectionIO[Int] =
    sql"DELETE FROM $table WHERE version_id = $versionId AND chunk_index >= $chunkCount".update.run

  override def findByVersionId(versionId: Long): ConnectionIO[List[RawBillTextDO]] =
    (fr"SELECT" ++ selectColumns ++
      fr"FROM $table WHERE version_id = $versionId ORDER BY chunk_index ASC")
      .query[RawBillTextDO]
      .to[List]

  override def countByVersionId(versionId: Long): ConnectionIO[Long] =
    sql"SELECT COUNT(*) FROM $table WHERE version_id = $versionId"
      .query[Long]
      .unique

}
