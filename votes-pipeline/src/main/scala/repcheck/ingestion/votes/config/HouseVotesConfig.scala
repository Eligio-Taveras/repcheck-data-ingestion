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
 *
 * ==Tuning `lookbackDays` — steady-state vs backfill==
 * The default (`7`) is tuned for the steady-state orchestrator cadence: run daily or weekly, keep only votes touched
 * inside the window, trust the next run to pick up anything that arrives later. Do NOT raise the default to cover
 * backfill — that silently bloats every steady-state run and burns the API quota.
 *
 * Two supported backfill strategies:
 *   1. One-off override: set `lookbackDays` to a large window in a dedicated config profile (e.g., `1825` for 5 years)
 *      and run the pipeline once against that profile. Revert to the steady-state profile afterwards. 2.
 *      Orchestrator-driven backfill: use the `pipeline.congress-sessions` tuple-list mechanism (per C6 execution plan
 *      decision 21x) to fan out one run per `(congress, session)` with `lookbackDays = 0` (filter disabled). This is
 *      preferred for the initial full-history load because it keeps each run bounded to one session.
 *
 * `0` or negative disables the filter entirely — keeps every paginated item regardless of `updateDate`. Use this ONLY
 * with the tuple-list backfill path above, never in a steady-state profile.
 */
final case class HouseVotesConfig(
  congress: Int,
  session: Int,
  parallelism: Int = 1,
  pageDelay: FiniteDuration = 2.seconds,
  lookbackDays: Int = 7,
) derives ConfigReader
