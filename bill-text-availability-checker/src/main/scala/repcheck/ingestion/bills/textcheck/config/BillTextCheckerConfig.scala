package repcheck.ingestion.bills.textcheck.config

import pureconfig.ConfigReader

import repcheck.pipeline.models.errors.RetryConfig

/**
 * Comma-separated list of congresses the availability checker will sweep, e.g. "103,104,...,119".
 *
 * Without this filter the sweep includes pre-103 bills whose `expected_text_version_code` was set by the metadata
 * pipeline's chamber-floor write but for which Congress.gov returns no text body — burning the Congress.gov rate budget
 * on no-op API calls. As a new congress opens (e.g. 120 in 2027), append it here or override via
 * `BILL_TEXT_CHECK_CONGRESSES`.
 *
 * Empty string means "no congress filter" (sweep everything). Use only for one-off backfills.
 */
final case class BillTextCheckerConfig(
  parallelism: Int,
  eventPublishRetry: RetryConfig,
  congresses: String,
) derives ConfigReader {

  /**
   * Parse the comma-separated `congresses` string into a `List[Int]`. Whitespace tolerated; non-numeric tokens silently
   * dropped.
   */
  def congressList: List[Int] =
    if (congresses.isEmpty) {
      List.empty
    } else {
      congresses.split(",").toList.flatMap(s => s.trim.toIntOption)
    }

}
