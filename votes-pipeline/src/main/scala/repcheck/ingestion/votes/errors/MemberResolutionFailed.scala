package repcheck.ingestion.votes.errors

/**
 * Raised by [[repcheck.ingestion.votes.pipeline.MemberResolver]] when a bioguide id cannot be resolved to a
 * `members.id` even after placeholder creation. Indicates a real defect in the placeholder flow — the `ensureExists`
 * insert succeeded but the subsequent `findByBioguideId` returned `None`, which should be impossible unless another
 * actor deleted the row between the two calls.
 *
 * The unique message anchor `"Failed to resolve member "` lets operator log-search locate every occurrence across the
 * run. `bioguideId` captures the upstream identifier so the vote can be re-ingested once the underlying member row is
 * restored.
 */
final case class MemberResolutionFailed(
  bioguideId: String,
  detail: String,
) extends Exception(s"Failed to resolve member $bioguideId: $detail")
