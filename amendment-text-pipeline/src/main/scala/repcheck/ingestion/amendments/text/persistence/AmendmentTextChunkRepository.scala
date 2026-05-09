package repcheck.ingestion.amendments.text.persistence

import repcheck.shared.models.congress.dos.amendment.AmendmentTextChunkDO

/**
 * Persistence boundary for `amendment_text_chunks` (db-migrations 040). Mirrors
 * [[repcheck.ingestion.bills.text.persistence.RawBillTextRepository]] for the amendment side. One row per chunk of an
 * amendment text version's text. Rows are idempotently re-written on re-processing: orphan chunks for a version are
 * DELETEd before re-streaming, then INSERTed via `insertMany` from the cross-amendment embedder.
 */
trait AmendmentTextChunkRepository[F[_]] {

  /**
   * Delete all chunks for the supplied amendment text version. Used by the streaming-INSERT flow to clear partial
   * chunks left by a previous failed run before re-streaming from scratch. Idempotent: a missing-row count is fine.
   */
  def deleteByVersionId(versionId: Long): F[Unit]

  /**
   * INSERT a batch of chunk rows in a single transaction. Same trade-off as the bill side: a single batch may span
   * multiple `version_id`s when the cross-amendment embedder mixes chunks from concurrently-processing amendments;
   * `replaceAll`-style DELETE-then-INSERT can't be used because the DELETE would clobber chunks of OTHER amendment
   * versions, and one-row-at-a-time `insertOne` is too chatty.
   *
   *   - Empty `rows` short-circuits to no-op.
   *   - `(version_id, chunk_index)` uniqueness applies (per migration 040 partial unique index); caller is responsible
   *     for `clearOrphanChunks` having run before submission.
   */
  def insertMany(rows: List[AmendmentTextChunkDO]): F[Unit]

  /**
   * Fetch every chunk attached to the supplied amendment text version, ordered by `chunk_index` so callers can
   * `mkString` to reconstruct the original document. Returns `Nil` if no chunks exist.
   */
  def findByVersionId(versionId: Long): F[List[AmendmentTextChunkDO]]

  /** Total chunk count attached to a version. Lighter than `findByVersionId` for status / summary use. */
  def countByVersionId(versionId: Long): F[Long]

  /**
   * Sum of `LENGTH(content)` across every chunk attached to this version. Used by the processor to populate
   * `amendment_text_versions.text_length` with the actual character count of the extracted text — without having to
   * thread a counter Ref through the streaming pipeline.
   */
  def sumContentLengthByVersionId(versionId: Long): F[Long]

}
