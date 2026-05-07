package repcheck.ingestion.amendments.observability

import java.util.concurrent.atomic.AtomicLong

/**
 * Process-local counters for the amendments-pipeline. These exist to give operators visibility into the L2 wasted-call
 * rate during backfill (parallel siblings under a shared parent each fetching the parent's detail before either
 * commits) and the recursion bottoming out without a hydrating bill (treaty / procedural orphan).
 *
 * The counters are intentionally thread-safe `AtomicLong`s rather than `Ref[F, Long]` — they're written from inside
 * `parEvalMap` workers across many fibers and read once at the end of the run for the summary log. There is no `F[_]`
 * dependency to surface.
 *
 * @note
 *   This is a process-local meter, not a Prometheus exposition. The §7.5/§7.6 production stack will eventually wire the
 *   numbers into Cloud Monitoring; in the meantime the summary log is the operator-facing surface.
 */
final class AmendmentMetrics {

  private val recursionRedundantFetches = new AtomicLong(0L)
  private val orphanResolved            = new AtomicLong(0L)
  private val recursionDepthExceeded    = new AtomicLong(0L)
  private val totalDetailFetches        = new AtomicLong(0L)
  private val congressOutOfRange        = new AtomicLong(0L)

  /**
   * Bump the redundant-detail-fetch counter when the recursion code path knowingly fetches a parent that another fiber
   * was already racing to fetch. Per L2 in the design: `ON CONFLICT` upsert prevents duplicate rows but the API call
   * itself is repeated; this counter surfaces the rate so operators can decide whether to invest in pre-resolution (P6)
   * or accept the steady-state cost.
   */
  def incrementRecursionRedundant(): Unit = {
    val _ = recursionRedundantFetches.incrementAndGet()
  }

  /** Bump every time `apiClient.fetchDetail` is invoked, regardless of cause. */
  def incrementDetailFetches(): Unit = {
    val _ = totalDetailFetches.incrementAndGet()
  }

  /**
   * Bump when a recursion frame returns with no resolved bill — typically a treaty / procedural amendment. Useful for
   * the summary log so an operator can sanity-check the orphan rate per run.
   */
  def incrementOrphanResolved(): Unit = {
    val _ = orphanResolved.incrementAndGet()
  }

  /** Bump when the depth guard trips. Should always be ~0 in a healthy backfill. */
  def incrementRecursionDepthExceeded(): Unit = {
    val _ = recursionDepthExceeded.incrementAndGet()
  }

  /**
   * Bump when a list-page item is filtered out because its `congress` falls outside `[congressesMin, congressesMax]`.
   * The global `/v3/amendment` endpoint returns rows from every congress edited within the lookback window; this
   * counter surfaces the rate at which we discard out-of-scope rows (most notably the pre-102 cutoff).
   */
  def incrementCongressOutOfRange(): Unit = {
    val _ = congressOutOfRange.incrementAndGet()
  }

  def snapshot(): AmendmentMetricsSnapshot =
    AmendmentMetricsSnapshot(
      detailFetches = totalDetailFetches.get(),
      recursionRedundantFetches = recursionRedundantFetches.get(),
      orphanResolved = orphanResolved.get(),
      recursionDepthExceeded = recursionDepthExceeded.get(),
      congressOutOfRange = congressOutOfRange.get(),
    )

}

object AmendmentMetrics {

  def make(): AmendmentMetrics = new AmendmentMetrics
}

/**
 * Read-only snapshot of an [[AmendmentMetrics]] at a point in time. Returned by [[AmendmentMetrics.snapshot]] so
 * callers (summary logger, tests) can observe values without holding a reference to the mutable counters.
 */
final case class AmendmentMetricsSnapshot(
  detailFetches: Long,
  recursionRedundantFetches: Long,
  orphanResolved: Long,
  recursionDepthExceeded: Long,
  congressOutOfRange: Long,
)
