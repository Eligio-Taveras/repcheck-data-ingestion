package repcheck.ingestion.amendments.text.embedding

import cats.effect.Deferred

import repcheck.pipeline.models.metadata.ProcessingResult

/**
 * Per-amendment context flowing alongside each chunk through the cross-amendment embedder. Mirror of the bill-side
 * [[repcheck.ingestion.bills.text.embedding.BillEmbedCtx]] but keyed on amendment surrogate id + version row id. Kept
 * small and immutable — every chunk in the embedder's queue carries one.
 */
final case class AmendmentEmbedCtx(
  amendmentId: Long,
  versionId: Long,
  naturalKey: String,
)

/** A single chunk submission flowing through the cross-amendment embedder's queue. */
final case class AmendmentChunkSubmission(
  ctx: AmendmentEmbedCtx,
  chunkIdx: Int,
  text: String,
)

/**
 * In-flight state for one amendment being processed by the cross-amendment embedder.
 *
 * @param ctx
 *   identifying info for the amendment (DB ids + natural key for the eventual ProcessingResult).
 * @param expected
 *   total chunk count the amendment will produce. `None` while the producer is still streaming chunks; becomes
 *   `Some(n)` when the producer's chunk stream terminates and the embedder is told the count.
 * @param persisted
 *   number of chunks for this amendment that have been INSERTed so far. Incremented inside an atomic state-transaction
 *   alongside the buffer flush that processed the amendment's chunks.
 * @param deferred
 *   the completion handle the amendment's `processChunks` is awaiting. Resolved by whichever producer's flush brings
 *   `persisted == expected`, OR by `finalizeSubmission` when both reach 0 (empty stream).
 */
final private[embedding] case class AmendmentEmbedProgress[F[_]](
  ctx: AmendmentEmbedCtx,
  expected: Option[Int],
  persisted: Int,
  deferred: Deferred[F, ProcessingResult],
) {

  /** True iff the producer finalized AND all of the amendment's chunks have been persisted. */
  def shouldComplete: Boolean = expected.contains(persisted)

}

/**
 * Atomic state for the foreground-only cross-amendment embedder. Mirror of the bill-side `EmbedderState` — keeping the
 * buffer and per-version progress map together in one Ref means a single `state.modify` can transactionally:
 *
 *   - add a chunk + decide whether to flush (in `offerChunk`)
 *   - increment `persisted` counters and remove fully-completed versions (in `applyBatchResult`)
 *   - drain the residual buffer + set `expected` (in `finalizeSubmission`)
 *
 * No background fiber, no `Queue`. Each producer's `offerChunk` is the only path that can flush, triggered when the
 * buffer hits `batchSize`. A producer's `finalizeSubmission` is the second flush trigger — it always force-flushes the
 * residual buffer to guarantee a small amendment's chunks are processed even if no other producer fills the buffer
 * afterward.
 *
 * The progress map is keyed by `versionId` rather than `amendmentId`. A single amendment can produce multiple
 * concurrent in-flight events — distinct `(version_type, format_type)` tuples (e.g., SUB vs MOD, or HTML vs PDF) all
 * map to distinct `amendment_text_versions.id` values, but to the same `amendmentId`. Keying on `versionId` keeps each
 * concurrent submission's Deferred isolated; without this, the second event's `register` would clobber the first's
 * progress entry and leave one of the producers waiting indefinitely.
 */
final private[embedding] case class AmendmentEmbedderState[F[_]](
  buffer: Vector[AmendmentChunkSubmission],
  amendments: Map[Long, AmendmentEmbedProgress[F]],
)

private[embedding] object AmendmentEmbedderState {
  def empty[F[_]]: AmendmentEmbedderState[F] = AmendmentEmbedderState[F](Vector.empty, Map.empty)
}
