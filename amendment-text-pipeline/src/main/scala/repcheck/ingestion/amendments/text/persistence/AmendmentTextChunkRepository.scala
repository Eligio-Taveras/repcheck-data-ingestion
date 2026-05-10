package repcheck.ingestion.amendments.text.persistence

import repcheck.shared.models.congress.dos.amendment.AmendmentTextChunkDO

/**
 * Persistence boundary for `amendment_text_chunks` (db-migrations 040). Mirrors
 * [[repcheck.ingestion.bills.text.persistence.RawBillTextRepository]] for the amendment side. One row per chunk of an
 * amendment text version's text.
 *
 * ==Idempotent UPSERT (last-writer-wins)==
 *
 * Re-deliveries are handled at the SQL layer via `INSERT ... ON CONFLICT (version_id, chunk_index) DO UPDATE` — see
 * [[upsertMany]]. The natural identity for a chunk is `(version_id, chunk_index)`; same identity → same Congress.gov
 * bytes → same chunks (the chunker is deterministic given source bytes), so the UPDATE branch overwrites with
 * effectively the same data on the happy path. There is no per-chunk `submission_version_date` gate; if Congress.gov
 * ever mutated published amendment text for a given `(amendment_id, version_type, format_type)` triple, recovery is
 * "re-emit the event" and the UPSERT overwrites.
 *
 * ==Stale-tail trim==
 *
 * Re-extraction with FEWER chunks (e.g. 100 → 90) leaves rows 90–99 from the prior run as dead data because the new
 * UPSERT only touches chunks 0–89. [[trimChunksPast]] DELETEs `chunk_index >= newCount` after a successful UPSERT batch
 * to keep the chunk set tight.
 */
trait AmendmentTextChunkRepository[F[_]] {

  /**
   * UPSERT a batch of chunks in a single transaction. Idempotent on `(version_id, chunk_index)` — re-runs (Pub/Sub
   * redelivery, post-crash retry) overwrite the existing row's `content` + `embedding` in place. Returns the number of
   * rows actually written by the statement; on the happy path every row writes (INSERT or UPDATE) so the count equals
   * `rows.size`.
   *
   *   - Empty `rows` short-circuits to `0`.
   *   - Mixed `version_id`s in a single batch are allowed — the cross-amendment embedder buffers chunks across
   *     concurrently-processing amendments and flushes them together.
   */
  def upsertMany(rows: List[AmendmentChunkRow]): F[Int]

  /**
   * DELETE chunks whose `chunk_index >= chunkCount` for the supplied version. Used to clean up the stale tail when a
   * re-submission produces fewer chunks than a previous run. Idempotent: a missing-row count is fine. Returns the
   * number of rows deleted.
   */
  def trimChunksPast(versionId: Long, chunkCount: Int): F[Int]

  /**
   * Fetch every chunk attached to the supplied amendment text version, ordered by `chunk_index` so callers can
   * `mkString` to reconstruct the original document. Returns `Nil` if no chunks exist.
   */
  def findByVersionId(versionId: Long): F[List[AmendmentTextChunkDO]]

  /** Total chunk count attached to a version. Lighter than `findByVersionId` for status / summary use. */
  def countByVersionId(versionId: Long): F[Long]

  /**
   * Sum of `LENGTH(content)` across every chunk attached to this version. Used by the embedder to populate
   * `amendment_text_versions.text_length` with the actual character count of the extracted text — without having to
   * thread a counter Ref through the streaming pipeline.
   */
  def sumContentLengthByVersionId(versionId: Long): F[Long]

}

/**
 * Row shape for [[AmendmentTextChunkRepository.upsertMany]]. Carries everything needed to write one chunk:
 * `amendment_id` (FK to amendments, NOT NULL per migration 040), `version_id` (FK to amendment_text_versions, NOT NULL
 * for chunks the embedder produces), `chunk_index`, `content`, and the embedding vector. Distinct from
 * [[AmendmentTextChunkDO]] which is the read-side shape with auto-generated `id` + `created_at` columns.
 */
final case class AmendmentChunkRow(
  amendmentId: Long,
  versionId: Long,
  chunkIndex: Int,
  content: String,
  embedding: Option[Array[Float]],
)
