package repcheck.ingestion.votes.errors

/**
 * Raised at pipeline startup when the `stepRunId` CLI argument is missing, blank, or not a valid `Long`. The launcher
 * is responsible for assigning a `workflow_run_steps.id` before invoking the pipeline and passing it through as
 * `args(2)` alongside the run-level identifier in `args(1)`; a missing or malformed value here means the launcher
 * contract was violated and the run should fail fast.
 */
final case class StepRunIdInvalid(rawValue: String)
    extends Exception(s"stepRunId argument invalid or missing: '$rawValue'")
