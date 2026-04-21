package repcheck.ingestion.votes.config

import scala.concurrent.duration._

import pureconfig.ConfigReader

/**
 * House-side votes configuration. Drives the `HouseVotesApiClient` — what Congress/session to fetch, how many detail
 * calls to run in parallel, the per-request inter-page delay enforced by the app-level `rateLimitedClient` wrapper, and
 * the client-side lookback window used to filter list results.
 *
 * The beta `/house-vote` endpoint does NOT accept `fromDateTime` / `toDateTime` / `sort` query params (those return
 * HTTP 400 on this endpoint even though they work on sibling endpoints like `/bill`). `lookbackDays` is therefore
 * applied client-side after fetching all pages for the target congress/session: sort DESC by `updateDate`, keep items
 * whose `updateDate >= now - lookbackDays`.
 *
 * @param congress
 *   target congress number (e.g. 119).
 * @param session
 *   session within the congress (1 or 2).
 * @param parallelism
 *   `parEvalMap` parallelism for detail-endpoint calls made from this one client. Per-request pacing is enforced by the
 *   `rateLimitedClient` wrapper in `VotesPipeline.scala` (`Semaphore(1)` + `pageDelay`), not by `parallelism` alone, so
 *   setting `parallelism > 1` still honors the request interval.
 * @param pageDelay
 *   minimum interval between successive requests on the House client. Wired into the `rateLimitedClient` semaphore
 *   release so every request waits at least this long after the previous one completes.
 * @param lookbackDays
 *   client-side filter window. Items whose `updateDate` is older than `now - lookbackDays` are dropped after all pages
 *   for the congress/session have been fetched. 0 or negative disables the filter (keeps everything).
 */
final case class HouseVotesConfig(
  congress: Int,
  session: Int,
  parallelism: Int = 1,
  pageDelay: FiniteDuration = 2.seconds,
  lookbackDays: Int = 7,
) derives ConfigReader
