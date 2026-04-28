package repcheck.ingestion.bills.text.embedding

import cats.effect.Deferred

import repcheck.pipeline.models.metadata.ProcessingResult

/**
 * Per-bill context carried alongside each chunk through the cross-bill embedder. Contains the keys needed to persist
 * the chunk's row (`dbBillId`, `versionId`) plus the bill's natural key for completion-result reporting.
 *
 * Kept small and immutable: every chunk in the embedder's queue carries one of these so the per-chunk overhead matters;
 * we don't put non-essential fields here.
 */
final case class BillEmbedCtx(
  dbBillId: Long,
  versionId: Long,
  naturalKey: String,
)

/** A single chunk submission flowing through the cross-bill embedder's queue. */
final case class ChunkSubmission(
  ctx: BillEmbedCtx,
  chunkIdx: Int,
  text: String,
)

/**
 * In-flight state for one bill being processed by the cross-bill embedder.
 *
 * @param ctx
 *   identifying info for the bill (DB ids + natural key for the eventual ProcessingResult).
 * @param expected
 *   total chunk count the bill will produce. `None` while the producer is still streaming chunks; becomes `Some(n)`
 *   when [[CrossBillEmbedder.finalizeSubmission]] is called after the bill's chunk stream terminates.
 * @param persisted
 *   number of chunks for this bill that have been INSERTed so far. Incremented inside an atomic state-transaction
 *   alongside the buffer flush that processed the bill's chunks.
 * @param deferred
 *   the completion handle the bill's processChunks is awaiting. Resolved by whichever producer's flush brings the
 *   bill's `persisted == expected`, OR by `finalizeSubmission` when both reach 0 (empty stream).
 */
final private[embedding] case class BillEmbedProgress[F[_]](
  ctx: BillEmbedCtx,
  expected: Option[Int],
  persisted: Int,
  deferred: Deferred[F, ProcessingResult],
) {

  /** True iff the producer finalized AND all of the bill's chunks have been persisted. */
  def shouldComplete: Boolean = expected.contains(persisted)

}

/**
 * Atomic state for the foreground-only cross-bill embedder.
 *
 * The shared `Ref[F, EmbedderState[F]]` holds both the unflushed-chunk buffer AND the per-bill progress map. Keeping
 * them in ONE Ref lets a single `state.modify` transactionally:
 *
 *   - add a chunk to the buffer + decide whether to flush in `offerChunk`
 *   - increment `persisted` counters for the bills in a flushed batch + remove fully-completed bills in
 *     `applyBatchResult`
 *   - drain the residual buffer + set `expected` for a bill in `finalizeSubmission`
 *
 * No background fiber, no Queue. Each producer's `offerChunk` is the only path that can flush, triggered when the
 * buffer hits `batchSize`. A producer's `finalizeSubmission` is the second flush trigger — it always force-flushes the
 * residual buffer to guarantee a bill's chunks are processed even if no other producer fills the buffer after it.
 *
 * @param buffer
 *   pending chunks waiting to be flushed. Bounded loosely by `batchSize` (a flush happens as soon as the buffer reaches
 *   that size on offer; finalize drains it regardless).
 * @param bills
 *   per-bill progress, keyed by `BillEmbedCtx.dbBillId`.
 */
final private[embedding] case class EmbedderState[F[_]](
  buffer: Vector[ChunkSubmission],
  bills: Map[Long, BillEmbedProgress[F]],
)

private[embedding] object EmbedderState {
  def empty[F[_]]: EmbedderState[F] = EmbedderState[F](Vector.empty, Map.empty)
}
