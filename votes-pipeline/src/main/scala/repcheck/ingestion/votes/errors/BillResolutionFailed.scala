package repcheck.ingestion.votes.errors

/**
 * Raised by [[repcheck.ingestion.votes.pipeline.BillResolver]] when a bill natural key cannot be resolved to a
 * `bills.id` even after placeholder creation. The same defensive pattern as [[MemberResolutionFailed]]: the
 * `ensureExists` path succeeded but the subsequent `findIdByNaturalKey` returned `None`, which indicates an external
 * actor mutated the row between the two calls.
 *
 * The unique message anchor `"Failed to resolve bill "` lets operator log-search locate every occurrence.
 * `billNaturalKey` captures the upstream identifier (e.g., "119-HR-1234") so the vote can be re-ingested once the
 * underlying bill row is restored.
 */
final case class BillResolutionFailed(
  billNaturalKey: String,
  detail: String,
) extends Exception(s"Failed to resolve bill $billNaturalKey: $detail")
