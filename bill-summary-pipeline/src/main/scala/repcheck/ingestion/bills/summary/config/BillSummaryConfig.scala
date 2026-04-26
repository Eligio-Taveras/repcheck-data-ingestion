package repcheck.ingestion.bills.summary.config

import scala.concurrent.duration.FiniteDuration

import pureconfig.ConfigReader

/**
 * Pipeline-specific configuration for `bill-summary-pipeline`.
 *
 * @param initialLookbackDays
 *   First-run-only lookback window (days). When `workflow_run_steps` has no successful prior run for `stepName`, the
 *   pipeline computes `fromDateTime = NOW() - initialLookbackDays`. Subsequent runs use the watermark from
 *   `WorkflowRunStepsRepository.lastSuccessfulEndAt` minus `watermarkBuffer`. Default `3650` (10 years) per the
 *   cross-pipeline policy that every paginated pipeline should run with at least 2-year history, preferring 10.
 * @param watermarkBuffer
 *   Subtracted from the previous run's end-time when computing `fromDateTime`. Catches summaries that updated AFTER the
 *   previous run's `toDateTime` cutoff but before the run finished. Default `5m`.
 * @param congresses
 *   Iterate these congresses sequentially within each tick via `/summaries/{congress}`. Configurable so backfill can
 *   broaden to historical congresses and steady-state can narrow to the active session. Order is preserved.
 * @param stepName
 *   The workflow_run_steps row key used for watermark queries. Keep stable across deploys — changing it forces a
 *   one-time fallback to the `initialLookbackDays` window.
 */
final case class BillSummaryConfig(
  initialLookbackDays: Int,
  watermarkBuffer: FiniteDuration,
  congresses: List[Int],
  stepName: String,
) derives ConfigReader
