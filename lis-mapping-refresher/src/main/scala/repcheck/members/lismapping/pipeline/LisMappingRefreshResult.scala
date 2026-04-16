package repcheck.members.lismapping.pipeline

/**
 * Summary of a single invocation of [[LisMappingProcessor.refreshAll]].
 *
 * Counts are cumulative across all senator entries parsed from the XML feed:
 *   - `totalParsed`: number of `SenatorLookupXmlDTO` rows consumed from the upstream stream (post-lookback-filter).
 *   - `inserted`: number of `MemberLisMappingDO` rows whose upsert produced a brand-new row (`xmax = 0`).
 *   - `updated`: number of `MemberLisMappingDO` rows whose upsert refreshed an existing row's `lastVerified`.
 *   - `eventsEmitted`: number of `MemberUpdatedEvent` messages published — always `<= inserted` because events are only
 *     emitted when the mapping was newly inserted AND a `members` row already exists for the bioguide id.
 */
final case class LisMappingRefreshResult(
  totalParsed: Int,
  inserted: Int,
  updated: Int,
  eventsEmitted: Int,
)

object LisMappingRefreshResult {

  val empty: LisMappingRefreshResult =
    LisMappingRefreshResult(totalParsed = 0, inserted = 0, updated = 0, eventsEmitted = 0)

}
