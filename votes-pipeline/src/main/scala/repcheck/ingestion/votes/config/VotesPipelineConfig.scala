package repcheck.ingestion.votes.config

import pureconfig.ConfigReader

/**
 * Pipeline-specific configuration for the votes pipeline.
 *
 * The pipeline iterates over a list of congresses, fanning out one fetch per (congress, session) tuple for both
 * chambers (sessions {1, 2} are always covered for each congress).
 *
 * `congresses` resolution at startup:
 *   - If non-empty (sourced from the `VOTES_CONGRESSES` env var, comma-separated, e.g. `"117,118,119"`), use the list
 *     verbatim.
 *   - If empty, the pipeline queries the `bills` table for `SELECT DISTINCT congress` and iterates over every congress
 *     that has at least one bill in the DB. This is the default — it lets the votes pipeline naturally follow the bills
 *     pipeline's coverage without manual config syncing.
 *
 * Old congresses without electronic vote data (House pre-100th ≈ 1988, Senate pre-101st ≈ 1989) return 404 from the
 * upstream APIs. The processor catches per-(congress, session) errors and continues with the next pair so a single 404
 * doesn't kill the whole run.
 */
final case class VotesPipelineConfig(
  house: HouseVotesConfig,
  senate: SenateVoteXmlConfig,
  congresses: List[Int] = Nil,
) derives ConfigReader
