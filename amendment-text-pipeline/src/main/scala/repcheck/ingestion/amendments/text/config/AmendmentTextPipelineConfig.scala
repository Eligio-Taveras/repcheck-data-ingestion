package repcheck.ingestion.amendments.text.config

import scala.concurrent.duration._

import pureconfig.ConfigReader

import repcheck.ingestion.common.api.HttpClientConfig

/**
 * Pipeline-level config for the amendment-text downloader. Mirror of
 * [[repcheck.ingestion.bills.text.config.BillTextPipelineConfig]]; same structure, same defaults conceptually.
 *
 * @param parallelism
 *   max concurrent amendments processed per run (parEvalMap fan-out).
 * @param downloadTimeoutSeconds
 *   per-request HTTP timeout for the underlying http4s client (legacy field — see `govInfoHttp.requestTimeout` for the
 *   client-level value actually applied to the GovInfo client).
 * @param pageDelay
 *   spacing between consecutive HTTP requests through the rate-limited client; controls the steady-state
 *   request-per-second ceiling for outbound traffic to api.govinfo.gov.
 * @param govInfoApiKey
 *   API key issued by GPO's govinfo.gov/api-signup. Required — amendment text bytes come from
 *   `api.govinfo.gov/packages/CREC-.../granules/CREC-.../htm?api_key=...` on the CREC mirror.
 * @param govInfoBaseUrl
 *   base URL for the GovInfo API. Defaults to the production endpoint; tests override with a WireMock URL.
 * @param govInfoPermits
 *   max in-flight requests through the rate-limited GovInfo client. `2L` is the bill-side empirical sweet spot (PR #83
 *   backfill experiment) — keeps the embedder GPU saturated without overshooting GovInfo's 36000-req/hour budget.
 *   Configurable so prod can dial up/down without a redeploy if the budget changes.
 * @param govInfoHttp
 *   timeouts + connection-pool sizing for the GovInfo Ember client. Defaults reuse the bill-side empirical values (120s
 *   request / 120s idle) — see `BillTextPipelineApp` scaladoc for the rationale.
 * @param ollamaHttp
 *   timeouts + connection-pool sizing for the Ollama embedder Ember client. Idle-connection time is shorter than the
 *   GovInfo client because local connections are cheap to re-establish; sharing this pool with GovInfo would let a
 *   leaked rate-limit permit wedge the embedder.
 */
final case class AmendmentTextPipelineConfig(
  parallelism: Int,
  downloadTimeoutSeconds: Int,
  pageDelay: FiniteDuration,
  govInfoApiKey: String,
  govInfoBaseUrl: String,
  govInfoPermits: Long = 2L,
  govInfoHttp: HttpClientConfig = HttpClientConfig(requestTimeout = 120.seconds, idleTimeout = 120.seconds),
  ollamaHttp: HttpClientConfig = HttpClientConfig(requestTimeout = 120.seconds, idleTimeout = 60.seconds),
) derives ConfigReader
