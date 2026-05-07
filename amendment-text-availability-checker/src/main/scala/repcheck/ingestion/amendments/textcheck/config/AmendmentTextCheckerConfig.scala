package repcheck.ingestion.amendments.textcheck.config

import scala.concurrent.duration._

import pureconfig.ConfigReader

import repcheck.pipeline.models.errors.RetryConfig

/**
 * Pipeline-level configuration for `amendment-text-availability-checker`.
 *
 * Drives the per-tick scan: every amendment with `congress >= minCongress` whose `last_text_check_at` is null OR older
 * than `staleAfter` is paged through `/v3/amendment/{c}/{type}/{number}/text` at `parallelism` in-flight requests.
 *
 * `minCongress` defaults to 117 because Congress.gov's amendment-text endpoint only returns text granules for the 117th
 * and later Congresses (per upstream docs). Earlier Congresses are explicitly excluded by the SQL pre-filter to avoid
 * burning the rate budget on calls guaranteed to be empty.
 */
final case class AmendmentTextCheckerConfig(
  minCongress: Int = 117,
  staleAfter: FiniteDuration = 4.hours,
  parallelism: Int = 4,
  pageDelay: FiniteDuration = 0.millis,
  pageSize: Int = 250,
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

  require(
    pageSize >= 1,
    s"pageSize ($pageSize) must be >= 1",
  )

}
