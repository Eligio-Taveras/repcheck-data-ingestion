package repcheck.ingestion.amendments.textcheck.config

import scala.concurrent.duration._

import pureconfig.ConfigReader

import com.repcheck.utils.errors.RetryConfig

/**
 * Pipeline-level configuration for `amendment-text-availability-checker`.
 *
 * Drives the per-tick scan: every amendment with `congress >= minCongress` whose `last_text_check_at` is null OR older
 * than `staleAfter` is fetched via `findCandidatesForTextCheck` (single SQL query, no paging) and dispatched through
 * `/v3/amendment/{c}/{type}/{number}/text` at `parallelism` in-flight requests.
 *
 * `minCongress` defaults to 117 because Congress.gov's amendment-text endpoint only returns text granules for the 117th
 * and later Congresses (per upstream docs). Earlier Congresses are explicitly excluded by the SQL pre-filter to avoid
 * burning the rate budget on calls guaranteed to be empty.
 *
 * Rate-limit settings (`pageDelay`, `pageSize`) live on [[CongressGovClientConfig]], NOT here — they govern the per-key
 * Congress.gov rate budget shared across all pipelines using that key, so they're a property of the API client, not of
 * any individual pipeline. Setting them here would have shadowed the real ones without being wired up.
 */
final case class AmendmentTextCheckerConfig(
  minCongress: Int = 117,
  staleAfter: FiniteDuration = 4.hours,
  parallelism: Int = 4,
  eventPublishRetry: RetryConfig = RetryConfig(),
) derives ConfigReader {

  require(
    minCongress >= 117,
    s"minCongress ($minCongress) must be >= 117 — Congress.gov serves amendment text only for 117th Congress forward",
  )

  require(
    staleAfter > Duration.Zero,
    s"staleAfter ($staleAfter) must be > 0",
  )

  require(
    parallelism >= 1,
    s"parallelism ($parallelism) must be >= 1",
  )

}
