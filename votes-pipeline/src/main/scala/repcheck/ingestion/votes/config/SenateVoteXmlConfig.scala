package repcheck.ingestion.votes.config

import scala.concurrent.duration.{DurationInt, FiniteDuration}

import pureconfig.ConfigReader

import repcheck.pipeline.models.errors.RetryConfig

/**
 * Configuration for the senate.gov Senate-vote XML client. Per the votes-pipeline execution plan (P2.2 decision 13):
 *   - `baseUrl` defaults to the senate.gov roll-call-list root so per-vote and index-feed URLs can be assembled by
 *     [[repcheck.ingestion.votes.xml.SenateVoteXmlClient]] without any additional prefix.
 *   - `parallelism = 1` (lower than the House API client; senate.gov is less tolerant of high request rates).
 *   - `requestDelay = 3.seconds` between requests — also more conservative than the House API side.
 *   - `retry` mirrors the conservative default retry policy used across ingestion-common XML feeds; overridable
 *     per-environment via `application.conf`.
 *
 * URL templates assembled by the client:
 *   - Vote: `{baseUrl}/vote_menu_{congress}_{session}/vote_{congress}_{session}_{voteNumber:05d}.xml`
 *   - Index: `{baseUrl}/vote_menu_{congress}_{session}.xml`
 */
final case class SenateVoteXmlConfig(
  baseUrl: String = "https://www.senate.gov/legislative/LIS/roll_call_lists",
  parallelism: Int = 1,
  requestDelay: FiniteDuration = 3.seconds,
  retry: RetryConfig = RetryConfig(
    maxRetries = 3,
    initialBackoffMs = 10L,
    maxBackoffMs = 60000L,
    backoffMultiplier = 2.0,
  ),
) derives ConfigReader
