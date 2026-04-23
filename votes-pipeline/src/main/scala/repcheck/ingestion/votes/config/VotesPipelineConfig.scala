package repcheck.ingestion.votes.config

import pureconfig.ConfigReader

/**
 * Pipeline-specific configuration for the votes pipeline. Top-level container for the two chamber configs plus any
 * future cross-chamber settings (historic backfill tuple list per C6 execution plan decision 21x will slot in here
 * alongside `house` / `senate`).
 *
 * The `congress` / `session` values live on the House sub-config intentionally — both chambers run against the same
 * session within one pipeline invocation. `VotesPipeline.buildProcessor` reads them from `house.congress` /
 * `house.session` and threads them into the processor for Senate stream construction too. If a future pipeline run ever
 * needs to fan out over multiple sessions, the backfill tuple list will sit at this top level rather than duplicating
 * the values into Senate config.
 */
final case class VotesPipelineConfig(
  house: HouseVotesConfig,
  senate: SenateVoteXmlConfig,
) derives ConfigReader
