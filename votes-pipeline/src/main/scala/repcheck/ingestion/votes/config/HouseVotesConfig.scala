package repcheck.ingestion.votes.config

import scala.concurrent.duration._

import pureconfig.ConfigReader

/**
 * House-side votes configuration. Drives the `HouseVotesApiClient` — how many detail calls to run in parallel, the
 * per-request inter-page delay enforced by the app-level `rateLimitedClient` wrapper, and the client-side lookback
 * window used to filter list results.
 *
 * The target Congress + session are NOT held here. Per P6.H5 the pipeline iterates over the resolved congress list
 * (from `VotesPipelineConfig.congresses` env or the `bills` table), fanning out one fetch per (congress, session)
 * tuple. Both arguments are passed to `HouseVotesApiClient.fetchRecentVotes(congress, session)` at call time.
 *
 * The beta `/house-vote` endpoint does NOT accept `fromDateTime` / `toDateTime` / `sort` query params (those return
 * HTTP 400 on this endpoint even though they work on sibling endpoints like `/bill`). `lookbackDays` is therefore
 * applied client-side after fetching all pages for the target congress/session: sort DESC by `updateDate`, keep items
 * whose `updateDate >= now - lookbackDays`.
 *
 * @param parallelism
 *   `parEvalMap` parallelism for detail-endpoint calls made from this one client. Pairs with `permits` below —
 *   `permits` sets the maximum number of in-flight HTTP requests, while `parallelism` sets the upstream fan-out
 *   queueing into the semaphore. Setting `parallelism > permits` just buffers callers behind the gate; setting
 *   `parallelism = permits` keeps the gate saturated.
 * @param permits
 *   maximum number of concurrent in-flight HTTP requests on the House client. Wired into the
 *   `RateLimitedHttpClient.make` semaphore. Default `1` preserves the original strictly-serial behaviour; raise (with
 *   care for the shared Congress.gov 5K/hr budget) to fan out detail-endpoint calls. Steady-state Ofelia ticks are
 *   bursty for a few minutes per cadence; backfills benefit from `4`–`8`.
 * @param pageDelay
 *   minimum interval each in-flight permit waits before being released back to the semaphore. With `permits = 1` this
 *   is effectively the gap between successive requests; with `permits > 1` it is the per-permit cooldown, so the
 *   sustained outbound rate is roughly `permits / pageDelay`.
 * @param lookbackDays
 *   client-side filter window. Items whose `updateDate` is older than `now - lookbackDays` are dropped after all pages
 *   for the (congress, session) have been fetched. 0 or negative disables the filter (keeps everything).
 *
 * ==Tuning `lookbackDays` — steady-state vs backfill==
 * The default (`7`) is tuned for the steady-state orchestrator cadence: run on the Ofelia tick, keep only votes touched
 * inside the window, trust the next run to pick up anything that arrives later. Do NOT raise the default to cover
 * backfill — that silently bloats every steady-state run and burns the API quota.
 *
 * For the initial full-history load, set `VOTES_LOOKBACK_DAYS=0` once (or a large window like 1825 for 5 years) so
 * every paginated item is kept regardless of `updateDate`, then revert to the default.
 */
final case class HouseVotesConfig(
  parallelism: Int = 1,
  permits: Int = 1,
  pageDelay: FiniteDuration = 2.seconds,
  lookbackDays: Int = 7,
) derives ConfigReader
