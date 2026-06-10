package repcheck.ingestion.votes.config

import scala.concurrent.duration.{DurationInt, FiniteDuration}

import pureconfig.ConfigReader

import com.repcheck.utils.errors.RetryConfig

/**
 * Configuration for the senate.gov Senate-vote XML client. Per the votes-pipeline execution plan (P2.2 decision 13):
 *   - `baseUrl` defaults to senate.gov's `.../legislative/LIS` root. [[repcheck.ingestion.votes.xml.SenateVoteUrls]]
 *     assembles the two distinct sub-paths on top: `{baseUrl}/roll_call_lists/vote_menu_{congress}_{session}.xml` for
 *     the per-session index, and
 *     `{baseUrl}/roll_call_votes/vote{congress}{session}/vote_{congress}_{session}_{voteNumber:05d}.xml` for individual
 *     votes. Senate.gov uses different sub-paths for the two — do not conflate them.
 *   - `parallelism = 1` (lower than the House API client; senate.gov is less tolerant of high request rates).
 *   - `permits = 1` — single in-flight request through the `RateLimitedHttpClient` semaphore. Raise carefully when
 *     backfilling: senate.gov is less tolerant than Congress.gov, but shares no budget with the rest of the pipelines
 *     so it can be tuned independently.
 *   - `requestDelay = 3.seconds` per-permit cooldown — pairs with `permits` to set the sustained outbound rate
 *     (`permits / requestDelay`).
 *   - `retry` mirrors the conservative default retry policy used across ingestion-common XML feeds; overridable
 *     per-environment via `application.conf`.
 */
final case class SenateVoteXmlConfig(
  baseUrl: String = "https://www.senate.gov/legislative/LIS",
  parallelism: Int = 1,
  permits: Int = 1,
  requestDelay: FiniteDuration = 3.seconds,
  retry: RetryConfig = RetryConfig(
    maxRetries = 3,
    initialBackoffMs = 10L,
    maxBackoffMs = 60000L,
    backoffMultiplier = 2.0,
  ),
) derives ConfigReader
