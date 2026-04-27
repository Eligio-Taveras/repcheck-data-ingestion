package repcheck.ingestion.members.profile.config

import scala.concurrent.duration.FiniteDuration

import pureconfig.ConfigReader

import repcheck.pipeline.models.errors.RetryConfig

/**
 * Member-profile pipeline configuration.
 *
 * @param congresses
 *   List of Congress numbers to ingest in a single run. Resolved at runtime by
 *   [[repcheck.ingestion.members.profile.app.MemberProfilePipeline.resolveCongresses]] in three layers (highest
 *   priority first):
 *   1. `MEMBERS_CONGRESSES` env var (comma-separated, e.g. `"117,118,119"`) — read via `sys.env` because HOCON cannot
 *      coerce a comma-separated string into `List[Int]`.
 *   1. This field, when populated by HOCON / test profiles.
 *   1. Fallback: `SELECT DISTINCT congress FROM bills` — lets members-pipeline mirror whatever congresses the bills
 *      pipeline has covered. The empty default makes the third layer the production wiring.
 *
 * Each congress is fetched via Congress.gov's `/v3/member/congress/{c}` endpoint sequentially; within a congress,
 * `parallelism` controls how many `processMember` fibers can run concurrently.
 * @param parallelism
 *   `parEvalMap` parallelism for per-member processing within a single congress.
 * @param pageDelay
 *   minimum interval between successive list-page requests on the Members API client (paired with the per-pipeline
 *   `RateLimitedHttpClient` semaphore in the IOApp wiring).
 * @param eventPublishRetry
 *   retry policy for `member.updated` Pub/Sub publish attempts.
 */
final case class MemberProfileConfig(
  congresses: List[Int],
  parallelism: Int,
  pageDelay: FiniteDuration,
  eventPublishRetry: RetryConfig,
) derives ConfigReader
