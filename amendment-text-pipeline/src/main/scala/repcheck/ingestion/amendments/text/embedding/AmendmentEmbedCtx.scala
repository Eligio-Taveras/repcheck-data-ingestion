package repcheck.ingestion.amendments.text.embedding

/**
 * Per-amendment context flowing alongside each chunk through the cross-amendment embedder. Mirror of the bill-side
 * `BillEmbedCtx` but keyed on amendment surrogate id + version row id. Kept small and immutable — every chunk in the
 * embedder's queue carries one.
 */
final case class AmendmentEmbedCtx(
  amendmentId: Long,
  versionId: Long,
  naturalKey: String,
)

/**
 * A single chunk submission flowing through the cross-amendment embedder's queue. `ackId` is carried so that the
 * flush-and-write path can attribute written rows back to the originating Pub/Sub message (multiple `ackId`s may
 * coexist in one batch when amendments interleave in the shared buffer).
 */
final case class AmendmentChunkSubmission(
  ctx: AmendmentEmbedCtx,
  chunkIdx: Int,
  text: String,
  ackId: String,
)

/**
 * In-flight per-`ackId` state for one Pub/Sub message being processed by the cross-amendment embedder.
 *
 * Two counters separate the ACK trigger from the side-effect trigger:
 *
 *   - `submitted` — count of chunks for this ackId that have been embedded + persisted in a successful flush.
 *     Incremented in `applyBatchResult` AFTER the embed + UPSERT succeeds, never at offer time. Drives ACK: when
 *     `submitted == expected`, every offered chunk has landed in the DB and the Pub/Sub ack fires.
 *   - `written` — of those, how many actually wrote rows (affected-row attribution from `upsertMany`). Drives trim +
 *     markFetched: only run them when `written > 0` so a no-op submission (e.g. one whose batch errored before reaching
 *     DB) doesn't falsely advance the version row.
 *
 * Failure handling: on a flush failure (embed error or UPSERT error) the ackId is removed from `acks` via `failBatch`
 * and any of its buffered chunks are purged from the shared buffer, so the completion check never fires. The producer's
 * `submit` separately handles stream errors via `failAck`, and cancellation via `guaranteeCase`. In all failure paths
 * NACK fires once and Pub/Sub redelivers (bounded by the subscription's `max_delivery_attempts` + dead-letter topic).
 *
 * On the happy path under last-writer-wins UPSERT, every offered chunk lands (INSERT or UPDATE) so `written ==
 * submitted` and both triggers fire.
 *
 * @param ack
 *   Pub/Sub acknowledge effect — invoked once when `submitted == expected`.
 * @param nack
 *   Pub/Sub explicit-redeliver effect — invoked on known failures (UPSERT error, embed error, trim error, markFetched
 *   error, producer stream error, producer cancellation).
 * @param expected
 *   None until the producer's `submit` finalizes; `Some(n)` after — `n` is the chunk count the stream produced.
 * @param submitted
 *   count of chunks for this ackId successfully embedded + persisted (incremented after a successful UPSERT batch).
 * @param written
 *   of those, the affected-row count from `upsertMany`. Drives trim + markFetched gating.
 */
final private[embedding] case class AmendmentAckProgress[F[_]](
  ackId: String,
  ctx: AmendmentEmbedCtx,
  ack: F[Unit],
  nack: F[Unit],
  expected: Option[Int],
  submitted: Int,
  written: Int,
) {

  /** True iff the producer finalized AND every offered chunk has reached terminal state (written or filtered). */
  def shouldComplete: Boolean = expected.contains(submitted)

}

/**
 * Atomic state for the foreground-only cross-amendment embedder. Mirror of the bill-side `EmbedderState` — keeping the
 * shared chunk buffer and per-ackId progress map together in one Ref means a single `state.modify` can transactionally:
 *
 *   - add a chunk + decide whether to flush (in `offerChunk`)
 *   - increment per-ackId counters (`submitted` always; `written` on UPSERT success) and remove fully-completed ackIds
 *     (in `applyBatchResult`)
 *   - drain the residual buffer + set `expected` (in `finalizeSubmission`)
 *
 * No background fiber, no `Queue`. Each producer's `offerChunk` is the only path that can flush, triggered when the
 * buffer hits `batchSize`. A producer's `finalizeSubmission` is the second flush trigger — it always force-flushes the
 * residual buffer to guarantee a small amendment's chunks are processed even if no other producer fills the buffer
 * afterward.
 *
 * The progress map is keyed by `ackId` rather than `versionId`. Two producers may legitimately race on the same
 * `versionId` (Pub/Sub at-least-once redelivery, or a fresh event landing while a previous one is still mid-flight).
 * Last-writer-wins UPSERT makes both writes safe at the DB layer; tracking per-ackId here lets each delivery's ACK fire
 * independently when its own chunks are accounted for.
 */
final private[embedding] case class AmendmentEmbedderState[F[_]](
  buffer: Vector[AmendmentChunkSubmission],
  acks: Map[String, AmendmentAckProgress[F]],
)

private[embedding] object AmendmentEmbedderState {
  def empty[F[_]]: AmendmentEmbedderState[F] = AmendmentEmbedderState[F](Vector.empty, Map.empty)
}
