package repcheck.ingestion.votes.metrics

import java.util.concurrent.atomic.AtomicLong

import cats.effect.Sync

/**
 * In-process counter that tracks how often amendment-placeholder creation was skipped because the vote's congress is
 * below `Constants.MinAmendmentCongress` (102). Used by both the Senate and House amendment-vote dispatch paths in
 * §7.4.
 *
 * Surfaced as a counter (not a log) so test assertions can pin the skip count directly. The repo has no central
 * `meterRegistry` yet — when one lands, swap this for a tagged `Counter`. Until then this is a simple `AtomicLong` so
 * the call site syntax (`counter.incPre102Skip(...)`) doesn't change later.
 *
 * The implementation wraps reads/writes in `Sync[F].delay` because [[increment]] mutates state — every consumer touches
 * it inside an `F[_]` block, never directly.
 */
class AmendmentPlaceholderSkipCounter {

  private val pre102: AtomicLong = new AtomicLong(0L)

  /** Increment the `reason="pre_102_amendment"` counter. Returns `F[Unit]` so callers compose in `for` blocks. */
  def incPre102Skip[F[_]](implicit F: Sync[F]): F[Unit] =
    F.delay { val _ = pre102.incrementAndGet(); () }

  /** Snapshot the current pre-102 skip count. Called from tests; production code never reads. */
  def pre102Skips: Long = pre102.get()

}
