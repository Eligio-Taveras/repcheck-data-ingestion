package repcheck.ingestion.bills.text.persistence

import repcheck.shared.models.congress.dos.bill.RawBillTextDO

/**
 * Persistence boundary for `raw_bill_text` (db-migrations 026). One row per chunk of a bill version's text. Rows are
 * idempotently re-written: `replaceAll(versionId, chunks)` deletes any prior chunks for that version and inserts the
 * new list inside a single transaction so re-processing doesn't leave a half-written intermediate state visible.
 *
 * The associated [[RawBillTextDO]] from shared-models 0.1.34+ models a row directly. `chunkIndex` is zero-based and
 * preserves document order — `ORDER BY chunk_index` reconstructs the original input verbatim.
 */
trait RawBillTextRepository[F[_]] {

  /**
   * Idempotent re-write of all chunks for a given bill text version. Implementations should:
   *   1. `DELETE FROM raw_bill_text WHERE version_id = $versionId`; 2. batch-insert the supplied chunks; both inside
   *      the same transaction so observers never see a partial chunk list. Empty `chunks` is allowed — it's a "delete
   *      all chunks for this version" call (used for fixture cleanup + tests).
   */
  def replaceAll(versionId: Long, chunks: List[RawBillTextDO]): F[Unit]

  /**
   * Delete all chunks for the supplied bill text version. Used by the streaming-INSERT flow to clear partial chunks
   * left by a previous failed run before re-streaming from scratch. Idempotent: a missing row count is fine.
   */
  def deleteByVersionId(versionId: Long): F[Unit]

  /**
   * INSERT exactly one chunk row. Composes with the streaming-INSERT flow in
   * [[repcheck.ingestion.bills.text.pipeline.BillTextProcessor]] which iterates the chunk list, embeds each, and
   * inserts one row per chunk in its own auto-committing transaction so peak heap stays bounded by one chunk's size +
   * its vector(1024) (~16 KB) instead of holding all chunks + all embeddings in heap before a bulk insert.
   *
   * Caller must have already cleared any orphan chunks for this `version_id` (see [[deleteByVersionId]]) — the unique
   * constraint `(version_id, chunk_index) WHERE version_id IS NOT NULL` would otherwise reject a duplicate
   * `chunk_index` left over from a prior partial run.
   */
  def insertOne(chunk: RawBillTextDO): F[Unit]

  /**
   * Fetch every chunk attached to the supplied bill version, ordered by `chunk_index` so callers can `mkString` to
   * reconstruct the original document. Returns `Nil` if no chunks exist.
   */
  def findByVersionId(versionId: Long): F[List[RawBillTextDO]]

  /**
   * Total chunk count attached to a bill version. Lighter than `findByVersionId` for status / summary use.
   */
  def countByVersionId(versionId: Long): F[Long]

}
