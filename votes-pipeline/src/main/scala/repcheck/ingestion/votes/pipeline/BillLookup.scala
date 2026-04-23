package repcheck.ingestion.votes.pipeline

import cats.effect.Async
import cats.syntax.all._

import doobie.implicits._
import doobie.util.transactor.Transactor

import repcheck.ingestion.bills.common.persistence.BillRepository
import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}
import repcheck.ingestion.common.placeholders.{EntityRepository, PlaceholderCreator}
import repcheck.ingestion.votes.errors.BillResolutionFailed
import repcheck.shared.models.congress.dos.bill.BillDO

/**
 * Produces the `billLookup` callback threaded into [[HouseVoteConverter.convert]] and [[SenateVoteConverter.convert]]
 * via `VoteMembersDTOOps.toDO(billLookup)`. Composes three ingredients the converters deliberately don't own:
 *   - `PlaceholderCreator.ensureExists[BillDO]` inserts a placeholder bill row (idempotent) if missing — the
 *     bills-pipeline enriches it on its next run.
 *   - `BillRepository.findByBillId` reads the surrogate id back in the same transactor.
 *   - If the id is still absent after ensureExists, [[BillResolutionFailed]] surfaces (another actor deleted the row
 *     between our two operations).
 *
 * The callback's signature is `String => F[Option[Long]]` for compatibility with the shared-models `billLookup`
 * contract, but in practice this implementation either returns `F.pure(Some(id))` or raises — never `F.pure(None)`. The
 * shared-models layer short-circuits `None` bill natural keys before invoking the callback (procedural votes).
 */
class BillLookup[F[_]: Async](
  billRepo: BillRepository[doobie.ConnectionIO],
  billEntityRepo: EntityRepository[F, BillDO],
  placeholderCreator: PlaceholderCreator[F],
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
        _       <- placeholderCreator.ensureExists[BillDO](nk, billEntityRepo)
        maybeId <- billRepo.findByBillId(nk).map(_.map(_.billId)).transact(xa)
        id <- maybeId match {
          case Some(id) => Async[F].pure(id)
          case None =>
            val err = BillResolutionFailed(nk, "findByBillId returned None after ensureExists")
            logger.error(logCtx, err.getMessage, Some(err)) *> Async[F].raiseError[Long](err)
        }
      } yield Some(id)

}
