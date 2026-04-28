package repcheck.ingestion.bills.text.config

import scala.concurrent.duration.FiniteDuration

import pureconfig.ConfigReader

/**
 * Pipeline-level config for the bill-text downloader.
 *
 * @param parallelism
 *   max concurrent bills processed per run (parEvalMap fan-out).
 * @param downloadTimeoutSeconds
 *   per-request HTTP timeout for the underlying http4s client.
 * @param pageDelay
 *   spacing between consecutive HTTP requests through the rate-limited client; controls the steady-state
 *   request-per-second ceiling for outbound traffic to api.govinfo.gov (and any other rate-limited host).
 * @param govInfoApiKey
 *   API key issued by GPO's govinfo.gov/api-signup. Required — bill text bytes now come from
 *   `api.govinfo.gov/packages/{packageId}/{format}?api_key=...` instead of the Cloudflare-fronted `www.congress.gov`
 *   site that bot-challenges raw HTTP clients.
 * @param govInfoBaseUrl
 *   base URL for the GovInfo API. Defaults to the production endpoint; tests override with a WireMock URL.
 */
final case class BillTextPipelineConfig(
  parallelism: Int,
  downloadTimeoutSeconds: Int,
  pageDelay: FiniteDuration,
  govInfoApiKey: String,
  govInfoBaseUrl: String,
) derives ConfigReader
