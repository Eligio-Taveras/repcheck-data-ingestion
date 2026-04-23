package repcheck.ingestion.votes.errors

/**
 * Raised when DTO→DO conversion for an incoming vote returns `Left(reason)` from
 * `VoteConversions.VoteMembersDTOOps.toDO` (House) or the votes-pipeline's own senate converter. Carries the vote's
 * natural key and the parser's rejection reason so operators can triage format drift in the upstream API response
 * without replaying the raw JSON/XML.
 *
 * The unique message anchor `"Conversion failed for vote "` keeps the string distinct from other vote-pipeline errors
 * so log-search filters do not alias on it.
 */
final case class VoteConversionFailed(
  naturalKey: String,
  detail: String,
) extends Exception(s"Conversion failed for vote $naturalKey: $detail")
