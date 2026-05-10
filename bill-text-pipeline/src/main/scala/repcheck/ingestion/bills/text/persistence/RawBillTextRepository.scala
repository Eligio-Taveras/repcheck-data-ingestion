package repcheck.ingestion.bills.text.persistence

import repcheck.shared.models.congress.dos.bill.RawBillTextDO

/**
 * Persistence boundary for `raw_bill_text` (db-migrations 026). One row per chunk of a bill version's text.
 *
 * Post-Option-C-refactor: writes are idempotent UPSERTs keyed on `(version_id, chunk_index)`. The previous `replaceAll`
 * / `deleteByVersionId` / `insertOne` / `insertMany` methods are gone — the new
 * [[repcheck.ingestion.bills.text.embedding.CrossBillEmbedder]] writes via [[upsertMany]] (last-writer-wins on the
 * conflict key) and prunes any leftover stale tail via [[trimChunksPast]] after each successful batch.
 *
 * Bills uses a plain UPSERT (no version-date gate) because `BillTextAvailableEvent` does not carry a `versionDate`
 * field and `bill_text_versions.version_date` is currently always written as `None`. See the corresponding
 * amendments-side repository for the gated variant.
 */
trait RawBillTextRepository[F[_]] {

  /**
   * Idempotent batch UPSERT keyed on `(version_id, chunk_index)`. INSERTs new rows; on conflict, overwrites `content` +
   * `embedding` with the new values (last-writer-wins). Returns the number of rows affected (each row counts as 1
   * whether it INSERTed or UPDATEd) so the embedder can populate its per-ackId `written` counter.
   *
   * Empty `rows` short-circuits to `0` without touching the DB.
   */
  def upsertMany(rows: List[RawBillTextDO]): F[Int]

  /**
   * Delete any chunks whose `chunk_index >= chunkCount` for the given `versionId`. Run after a successful UPSERT batch
   * to prune leftover chunks from a prior run that produced more chunks than the current submission. Idempotent — a
   * no-op when no stale tail exists. Returns the number of rows deleted.
   */
  def trimChunksPast(versionId: Long, chunkCount: Int): F[Int]

  /**
   * Fetch every chunk attached to the supplied bill version, ordered by `chunk_index` so callers can `mkString` to
   * reconstruct the original document. Returns `Nil` if no chunks exist.
   */
  def findByVersionId(versionId: Long): F[List[RawBillTextDO]]

  /** Total chunk count attached to a bill version. Lighter than `findByVersionId` for status / summary use. */
  def countByVersionId(versionId: Long): F[Long]

}
