package repcheck.ingestion.amendments.errors

/**
 * Raised by `AmendmentProcessor.processAmendment` when the recursive parent-amendment chase exceeds
 * `config.maxRecursionDepth`. Per §7.3 S1, this is the only safety net for runaway recursion — there is no in-flight
 * cycle set, because Congress.gov can't structurally produce parent cycles. Real legislative chains rarely exceed depth
 * 3–4; depth 10 is the practical bound.
 *
 * The exception is converted into `ProcessingResult.Failed` at the top of the per-amendment flow so a single corrupt
 * chain does not abort the streaming run.
 */
final case class AmendmentRecursionTooDeep(
  depth: Int,
  naturalKey: String,
) extends Exception(
      s"Amendment recursion exceeded max depth $depth at $naturalKey"
    )

/**
 * Raised when the per-amendment flow inside `AmendmentProcessor` fails for any reason that the processor wants to
 * surface as a `ProcessingResult.Failed`. Wraps the underlying cause so the standard `RetryWrapper` / `ErrorClassifier`
 * machinery can introspect the chain.
 */
final case class AmendmentProcessingFailed(
  naturalKey: String,
  cause: Throwable,
) extends Exception(
      s"Failed to process amendment $naturalKey: ${cause.getMessage}",
      cause,
    )

/**
 * Raised at boot by `AmendmentsPipeline.runWithFactories` when `database.maxConnections < parallelism ×
 * maxRecursionDepth + 5` (per P1). An undersized pool deadlocks a deep cold chain during recursion — every pending
 * sponsor / bill / parent resolution holds one connection. Failing fast at boot keeps the misconfiguration loud.
 */
final case class PoolSizingTooSmall(detail: String)
    extends Exception(s"Connection pool sized below recursion needs: $detail")
