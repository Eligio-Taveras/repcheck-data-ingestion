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
   * Fetch every chunk attached to the supplied bill version, ordered by `chunk_index` so callers can `mkString` to
   * reconstruct the original document. Returns `Nil` if no chunks exist.
   */
  def findByVersionId(versionId: Long): F[List[RawBillTextDO]]

  /**
   * Total chunk count attached to a bill version. Lighter than `findByVersionId` for status / summary use.
   */
  def countByVersionId(versionId: Long): F[Long]

}
