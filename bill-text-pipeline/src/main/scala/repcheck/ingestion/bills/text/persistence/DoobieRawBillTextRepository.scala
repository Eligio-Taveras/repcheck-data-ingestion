package repcheck.ingestion.bills.text.persistence

import doobie._
import doobie.implicits._
import doobie.postgres.implicits._

import repcheck.ingestion.bills.common.persistence.DoobieInstances.{floatArrayGet, floatArrayPut}
import repcheck.shared.models.congress.dos.bill.RawBillTextDO

/**
 * Doobie implementation of [[RawBillTextRepository]] keyed at `ConnectionIO`.
 *
 * `replaceAll` is the canonical write path: a single `ConnectionIO` that DELETEs by `version_id` then batch-inserts the
 * new chunks. Callers compose it with the parent version's INSERT/UPDATE under one `xa.trans` so a downstream failure
 * (e.g. event publish) rolls back the entire (version, chunks) tuple together.
 *
 * The pgvector `embedding` column is written via the `::vector` cast on the parameter — Doobie's `floatArrayPut` from
 * `bills-common.DoobieInstances` formats `Array[Float]` as `[1.0,2.0,...]` and PostgreSQL rejects the literal without
 * the explicit cast. Reads use `floatArrayGet` which parses the same shape back into `Array[Float]`.
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

  // Update[] needs a literal SQL string (not a Fragment) so the table-name constant has to be inlined here.
  private val insertSql: String =
    s"INSERT INTO $tableName (bill_id, version_id, chunk_index, content, embedding) VALUES (?, ?, ?, ?, ?::vector)"

  override def replaceAll(versionId: Long, chunks: List[RawBillTextDO]): ConnectionIO[Unit] = {
    val deleteOldChunks: ConnectionIO[Int] =
      sql"DELETE FROM $table WHERE version_id = $versionId".update.run

    val rowsForUpdate: List[(Long, Option[Long], Int, String, Option[Array[Float]])] =
      chunks.map(chunk => (chunk.billId, chunk.versionId, chunk.chunkIndex, chunk.content, chunk.embedding))

    val insertNewChunks: ConnectionIO[Int] =
      Update[(Long, Option[Long], Int, String, Option[Array[Float]])](insertSql).updateMany(rowsForUpdate)

    deleteOldChunks.flatMap { _ =>
      if (chunks.isEmpty) doobie.free.connection.unit
      else insertNewChunks.map(_ => ())
    }
  }

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
