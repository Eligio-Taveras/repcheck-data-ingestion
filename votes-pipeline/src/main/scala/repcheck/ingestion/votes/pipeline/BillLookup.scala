package repcheck.ingestion.votes.pipeline

import cats.effect.Async
import cats.syntax.all._

import doobie.implicits._
import doobie.util.transactor.Transactor

import repcheck.ingestion.bills.common.persistence.BillRepository
import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}
import repcheck.ingestion.votes.errors.BillResolutionFailed

/**
 * Produces the `billLookup` callback threaded into [[HouseVoteConverter.convert]] and [[SenateVoteConverter.convert]]
 * via `VoteMembersDTOOps.toDO(billLookup)`. Delegates the idempotent placeholder insert to the bills-common
 * [[BillRepository.upsertPlaceholder]] method — one home for all bills-table writes — then re-reads the surrogate id
 * and returns it.
 *
 * The callback's signature is `String => F[Option[Long]]` for compatibility with the shared-models `billLookup`
 * contract, but in practice this implementation either returns `F.pure(Some(id))` or raises — never `F.pure(None)`. The
 * shared-models layer short-circuits `None` bill natural keys before invoking the callback (procedural votes).
 *
 * ==Failure modes==
 *
 *   - Malformed natural key (not `"<congress>-<TYPE>-<number>"`): `BillRepository.upsertPlaceholder` raises
 *     `InvalidBillNaturalKey` from bills-common. The vote fails per-vote isolation as `ProcessingResult.Failed`.
 *   - Post-upsert `findByBillId` returns `None` (another actor deleted the row between our two operations):
 *     [[BillResolutionFailed]] surfaces with the caller's log context.
 */
class BillLookup[F[_]: Async](
  billRepo: BillRepository[doobie.ConnectionIO],
  xa: Transactor[F],
  logger: PipelineLogger[F],
) {

  /**
   * Bind a [[LogContext]] into the lookup callback so error logs carry the surrounding run/vote context. Returns a
   * function suitable for direct injection into the converters' `convert(dto, billLookup, logCtx)` entry points.
   */
  def forContext(logCtx: LogContext): String => F[Option[Long]] =
    nk =>
      for {
        _       <- billRepo.upsertPlaceholder(nk).transact(xa)
        maybeId <- billRepo.findByBillId(nk).map(_.map(_.billId)).transact(xa)
        id <- maybeId match {
          case Some(id) => Async[F].pure(id)
          case None =>
            val err = BillResolutionFailed(nk, "findByBillId returned None after upsertPlaceholder")
            logger.error(logCtx, err.getMessage, Some(err)) *> Async[F].raiseError[Long](err)
        }
      } yield Some(id)

}
