package repcheck.ingestion.amendments.config

import scala.concurrent.duration._

import pureconfig.ConfigReader

import repcheck.pipeline.models.constants.Constants

/**
 * Pipeline-level configuration for `amendments-pipeline`.
 *
 * Drives the per-tick scan window: every Congress in `[congressesMin, congressesMax]` is paged through
 * `/v3/amendment/{congress}` with `fromDateTime = now - lookbackDays`. The processor (§7.3) consumes the resulting
 * stream of `AmendmentListItemDTO` at `parallelism` in-flight detail fetches.
 *
 * `congressesMin` is bounded below by [[Constants.MinAmendmentCongress]] (102) — Congress.gov's amendments API does not
 * return data for earlier sessions and the schema's CHECK constraint rejects them.
 *
 * @param congressesMin
 *   Inclusive lower bound for the Congress range. Defaults to `MinAmendmentCongress` (102).
 * @param congressesMax
 *   Inclusive upper bound for the Congress range. Defaults to 119 (current).
 * @param lookbackDays
 *   Window subtracted from `now` to compute `fromDateTime` on each list-page request. `7` keeps the steady-state scan
 *   tight; bump it for backfill runs.
 * @param parallelism
 *   In-flight amendment-detail fetches in the processor pipeline. `4` is a polite default given the shared API key.
 * @param pageDelay
 *   Inter-page delay enforced by `CongressGovPaginatedClient`. `0.millis` lets the rate-limited HTTP client wrapper do
 *   the throttling.
 * @param maxRecursionDepth
 *   Reserved for the processor's parent-amendment chase: how deep into amendments-of-amendments to follow before giving
 *   up. `10` is loose; production data rarely exceeds depth 3.
 * @param pageSize
 *   Items requested per list page. `250` is the Congress.gov maximum.
 */
final case class AmendmentsConfig(
  congressesMin: Int = Constants.MinAmendmentCongress,
  congressesMax: Int = 119,
  lookbackDays: Int = 7,
  parallelism: Int = 4,
  pageDelay: FiniteDuration = 0.millis,
  maxRecursionDepth: Int = 10,
  pageSize: Int = 250,
) derives ConfigReader {

  require(
    congressesMin >= Constants.MinAmendmentCongress,
    s"congressesMin ($congressesMin) must be >= ${Constants.MinAmendmentCongress} (Constants.MinAmendmentCongress)",
  )

  require(
    congressesMax >= congressesMin,
    s"congressesMax ($congressesMax) must be >= congressesMin ($congressesMin)",
  )

  require(
    lookbackDays >= 0,
    s"lookbackDays ($lookbackDays) must be >= 0",
  )

  require(
    parallelism >= 1,
    s"parallelism ($parallelism) must be >= 1",
  )

  require(
    maxRecursionDepth >= 0,
    s"maxRecursionDepth ($maxRecursionDepth) must be >= 0",
  )

  require(
    pageSize >= 1,
    s"pageSize ($pageSize) must be >= 1",
  )

  def congresses: Range = congressesMin to congressesMax
}
