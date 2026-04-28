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
 *   total chunk count the bill will produce. `None` while the producer is still streaming chunks into the queue;
 *   becomes `Some(n)` when [[CrossBillEmbedder.finalizeSubmission]] is called after the chunkPipe stream terminates.
 * @param persisted
 *   number of chunks for this bill that have been INSERTed by the worker so far.
 * @param deferred
 *   the completion handle the bill's processEvent is awaiting.
 */
final private[embedding] case class BillEmbedProgress[F[_]](
  ctx: BillEmbedCtx,
  expected: Option[Int],
  persisted: Int,
  deferred: Deferred[F, ProcessingResult],
) {

  /**
   * True iff the producer has finalized submission AND the worker has persisted that many chunks. Used by both
   * [[CrossBillEmbedder.finalizeSubmission]] (when the producer arrives last) and
   * [[CrossBillEmbedder.incrementAndMaybeComplete]] (when the worker arrives last) to detect the same condition.
   */
  def shouldComplete: Boolean = expected.contains(persisted)

}
