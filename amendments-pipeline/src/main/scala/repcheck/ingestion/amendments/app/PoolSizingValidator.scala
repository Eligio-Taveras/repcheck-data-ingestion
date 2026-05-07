package repcheck.ingestion.amendments.app

import repcheck.ingestion.amendments.config.AmendmentsConfig
import repcheck.ingestion.common.db.DatabaseConfig

/**
 * P1 connection-pool pre-flight validator. Lives in its own file so it stays inside coverage even though
 * `AmendmentsPipelineResources` (the production resource wiring) is excluded from the coverage gate as pure wiring.
 *
 * `maxConnections` must be at least `parallelism × maxRecursionDepth + 5` so a deep cold chain at full parallelism has
 * connection headroom for sponsor/bill/parent resolutions plus a small bookkeeping buffer for the workflow-state
 * writer. Failing fast at boot is documented as the chosen behaviour — auto-correcting silently would mask a
 * misconfiguration that the operator should fix.
 */
private[app] object PoolSizingValidator {

  /**
   * @return
   *   `Some(message)` describing the violation, or `None` when the pool is sized correctly.
   */
  def validate(database: DatabaseConfig, pipeline: AmendmentsConfig): Option[String] = {
    val required = pipeline.parallelism * pipeline.maxRecursionDepth + 5
    if (database.maxConnections < required) {
      Some(
        s"database.maxConnections=${database.maxConnections.toString} is below the required " +
          s"parallelism (${pipeline.parallelism.toString}) × maxRecursionDepth " +
          s"(${pipeline.maxRecursionDepth.toString}) + 5 = ${required.toString} (per P1)."
      )
    } else { None }
  }

}
