package repcheck.ingestion.amendments.text.config

import scala.concurrent.duration.FiniteDuration

import pureconfig.ConfigReader

/**
 * Pipeline-level config for the amendment-text downloader. Mirror of
 * [[repcheck.ingestion.bills.text.config.BillTextPipelineConfig]]; same structure, same defaults conceptually.
 *
 * @param parallelism
 *   max concurrent amendments processed per run (parEvalMap fan-out).
 * @param downloadTimeoutSeconds
 *   per-request HTTP timeout for the underlying http4s client.
 * @param pageDelay
 *   spacing between consecutive HTTP requests through the rate-limited client; controls the steady-state
 *   request-per-second ceiling for outbound traffic to api.govinfo.gov.
 * @param govInfoApiKey
 *   API key issued by GPO's govinfo.gov/api-signup. Required — amendment text bytes come from
 *   `api.govinfo.gov/packages/CREC-.../granules/CREC-.../htm?api_key=...` on the CREC mirror.
 * @param govInfoBaseUrl
 *   base URL for the GovInfo API. Defaults to the production endpoint; tests override with a WireMock URL.
 */
final case class AmendmentTextPipelineConfig(
  parallelism: Int,
  downloadTimeoutSeconds: Int,
  pageDelay: FiniteDuration,
  govInfoApiKey: String,
  govInfoBaseUrl: String,
) derives ConfigReader
