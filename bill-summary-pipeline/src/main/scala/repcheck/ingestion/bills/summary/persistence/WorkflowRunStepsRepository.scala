package repcheck.ingestion.bills.summary.persistence

import java.time.Instant

import doobie._
import doobie.implicits._
import doobie.postgres.implicits._

import repcheck.pipeline.models.constants.Tables

/**
 * Read-only access to `workflow_run_steps` for watermark computation. Each pipeline writes its run rows via
 * `PipelineExecutor` (or equivalent); this trait exposes only the watermark query that bill-summary-pipeline (and any
 * future delta-fetch pipeline) needs to compute `fromDateTime` for incremental pulls.
 *
 * Lives in this subproject for now. If a second pipeline ever needs the same query it can be promoted to
 * ingestion-common — until then YAGNI says keep the surface area local.
 */
trait WorkflowRunStepsRepository[F[_]] {

  /**
   * Return the `completed_at` timestamp of the most-recent successful run for `stepName`. Returns `None` when there's
   * no prior successful run (first-ever invocation, or the step name was just renamed). Callers fall back to a
   * configured initial-lookback window in that case.
   *
   * Uses `status::text IN ('completed', 'completed_with_errors')` to handle the post-migration-017a enum value: a run
   * that finished but had per-item failures still establishes a watermark — the next run only needs to look back as far
   * as the previous run's wall-clock end time, regardless of its per-item success rate.
   */
  def lastSuccessfulEndAt(stepName: String): F[Option[Instant]]

}

class DoobieWorkflowRunStepsRepository extends WorkflowRunStepsRepository[ConnectionIO] {

  private val table = Fragment.const(Tables.WorkflowRunSteps)

  override def lastSuccessfulEndAt(stepName: String): ConnectionIO[Option[Instant]] =
    sql"""SELECT MAX(completed_at)
          FROM $table
          WHERE step_name = $stepName
            AND status::text IN ('completed', 'completed_with_errors')
            AND completed_at IS NOT NULL"""
      .query[Option[Instant]]
      .option
      .map(_.flatten)

}
