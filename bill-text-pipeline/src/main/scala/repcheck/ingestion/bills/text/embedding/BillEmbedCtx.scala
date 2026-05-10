package repcheck.ingestion.bills.text.embedding

/**
 * Per-bill context carried alongside each chunk through the cross-bill embedder. Contains the keys needed to persist
 * the chunk's row (`dbBillId`, `versionId`) plus the bill's natural key for diagnostics.
 *
 * Kept small and immutable: every chunk in the embedder's queue carries one of these so the per-chunk overhead matters;
 * we don't put non-essential fields here.
 *
 * Option C note: bills do NOT carry a `submissionVersionDate`. `BillTextAvailableEvent` has no `versionDate` field
 * today and `bill_text_versions.version_date` is always written as `None`, so the version-date-gated UPSERT cannot be
 * wired without an upstream pipeline-models bump. Bills uses last-writer-wins UPSERT instead.
 */
final case class BillEmbedCtx(
  dbBillId: Long,
  versionId: Long,
  naturalKey: String,
)

/**
 * A single chunk submission flowing through the cross-bill embedder's queue. `ackId` is the Pub/Sub ack id of the
 * message whose processing produced this chunk; the embedder uses it to credit the per-ackId `submitted` / `written`
 * counters and to ack/nack at completion.
 */
final case class ChunkSubmission(
  ctx: BillEmbedCtx,
  chunkIdx: Int,
  text: String,
  ackId: String,
)

/**
 * Per-Pub/Sub-message progress tracked by the cross-bill embedder. Multiple submissions for the same `versionId` are
 * tracked under their distinct ackIds — the previous "one Deferred per versionId" dedup is gone, replaced by SQL-level
 * idempotency (UPSERT on `(version_id, chunk_index)`) and bounded redelivery via Pub/Sub's dead-letter policy.
 *
 * @param ackId
 *   the Pub/Sub ack id; identifies the message uniquely.
 * @param ctx
 *   the bill's identifying info (versionId for trim+markFetched, naturalKey for logs).
 * @param ack
 *   the effect that calls `subscriber.acknowledge(ackId)`. Fires when `submitted == expected` — i.e., every chunk this
 *   producer offered has been embedded + persisted in a successful flush.
 * @param nack
 *   the effect that signals Pub/Sub to redeliver. Fires on known failures (embed error, UPSERT error, trim error,
 *   markFetched error, stream error, fiber cancellation). On batch failure the ackId is removed from `acks` entirely so
 *   this completion check never fires; the NACK path takes over.
 * @param expected
 *   total chunk count the producer offered. `None` while the chunk stream is still being drained; `Some(n)` after
 *   `finalizeSubmission`. ACK fires when `submitted` catches up to `expected`.
 * @param submitted
 *   chunks for this ackId that have been embedded + persisted in a successful flush. Incremented in `applyBatchSuccess`
 *   (never at offer time) — moving the increment to offer time would let ACK fire while writes were still in flight.
 * @param written
 *   chunks for this ackId that actually wrote to the DB (UPSERT affected_rows). Drives trim + markFetched.
 */
final private[embedding] case class AckProgress[F[_]](
  ackId: String,
  ctx: BillEmbedCtx,
  ack: F[Unit],
  nack: F[Unit],
  expected: Option[Int],
  submitted: Int,
  written: Int,
) {

  /** True iff the producer finalized AND every chunk it offered has been processed by some flush. */
  def shouldAck: Boolean = expected.contains(submitted)

}

/**
 * Atomic state for the foreground-only cross-bill embedder.
 *
 * The shared `Ref[F, EmbedderState[F]]` holds both the unflushed-chunk buffer AND the per-ackId progress map. Keeping
 * them in ONE Ref lets a single `state.modify` transactionally:
 *
 *   - add a chunk to the buffer + decide whether to flush in `offerChunk`
 *   - increment `submitted` and `written` counters for ackIds in a flushed batch + collect newly ack-able ackIds in
 *     `applyBatchResult` / `failBatch`
 *   - drain the residual buffer + set `expected` for an ackId in `finalizeSubmission`
 *
 * No background fiber, no Queue. Each producer's `offerChunk` is the only path that can flush (when the buffer hits
 * `batchSize`). A producer's `finalizeSubmission` is the second flush trigger — it always force-flushes the residual
 * buffer to guarantee an ackId's chunks are processed even if no other producer fills the buffer afterward.
 *
 * @param buffer
 *   pending chunks waiting to be flushed. Bounded loosely by `batchSize` (a flush happens as soon as the buffer reaches
 *   that size on offer; finalize drains it regardless).
 * @param acks
 *   per-ackId progress, keyed by `ackId`.
 */
final private[embedding] case class EmbedderState[F[_]](
  buffer: Vector[ChunkSubmission],
  acks: Map[String, AckProgress[F]],
)

private[embedding] object EmbedderState {
  def empty[F[_]]: EmbedderState[F] = EmbedderState[F](Vector.empty, Map.empty)
}
