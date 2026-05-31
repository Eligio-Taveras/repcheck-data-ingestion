package repcheck.members.committees.config

import pureconfig.ConfigReader

/**
 * Config for the historical committee-membership backfill from the GovInfo Congressional Directory (CDIR).
 *
 * The loader backfills the `lookbackCongresses` most recent congresses ending at `currentCongress` (inclusive) — e.g.
 * currentCongress=119, lookbackCongresses=5 covers 115–119 (~10 years). Set lookbackCongresses high to widen.
 */
final case class HistoricalLoaderConfig(
  currentCongress: Int,
  lookbackCongresses: Int,
) derives ConfigReader {

  /** Congresses to backfill, newest first. */
  def targetCongresses: List[Int] = {
    val oldest = math.max(1, currentCongress - lookbackCongresses + 1)
    (currentCongress to oldest by -1).toList
  }

}
