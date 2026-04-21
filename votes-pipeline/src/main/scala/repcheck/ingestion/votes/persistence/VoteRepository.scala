package repcheck.ingestion.votes.persistence

import repcheck.shared.models.congress.dos.vote.VoteDO

/**
 * Contract for reading and writing [[VoteDO]] rows in AlloyDB.
 *
 * Defined in this P2.5 branch as the minimum surface P2.5 ([[repcheck.ingestion.votes.pipeline.VoteChangeDetector]])
 * requires. The canonical implementation + any additional methods (e.g. upsert, findByBillId, findByCongress) lands
 * with P2.4 (`feat/votes-repositories`). Both branches target the same trait; when P2.4 merges, this trait stays stable
 * so P2.5's callers do not change — P2.4 only grows the surface.
 *
 * Lookup is by `natural_key` (e.g. `"119-house-42"` / `"119-senate-7"`) because that is the identity the change
 * detector receives from [[repcheck.shared.models.congress.dos.results.VoteConversionResult]] before any DB-assigned
 * BIGSERIAL PK is known.
 */
trait VoteRepository[F[_]] {

  /**
   * Look up a stored vote by its `natural_key`. Returns `None` when no row matches — that's the signal for
   * [[repcheck.ingestion.votes.pipeline.VoteChangeDetector]] to emit
   * [[repcheck.ingestion.votes.pipeline.VoteChangeReport.New]].
   */
  def findByNaturalKey(naturalKey: String): F[Option[VoteDO]]

}
