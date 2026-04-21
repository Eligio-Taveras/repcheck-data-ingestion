package repcheck.ingestion.votes.config

import pureconfig.ConfigReader

/**
 * Pipeline-specific configuration for the votes pipeline. Scaffold-minimal shape matching the bill-metadata-pipeline
 * precedent (`lookbackDays` + `parallelism`). Phase 2 (P2.1 / P2.2) will extend this with House- and Senate-specific
 * sub-configs (per-client `pageDelay` / `requestDelay`, Senate XML base URL, congress-sessions tuple list, etc.) per
 * the votes-pipeline execution plan.
 */
final case class VotesPipelineConfig(
  lookbackDays: Int,
  parallelism: Int,
) derives ConfigReader
