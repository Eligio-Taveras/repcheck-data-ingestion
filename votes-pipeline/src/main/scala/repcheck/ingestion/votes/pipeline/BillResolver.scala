package repcheck.ingestion.votes.pipeline

import cats.effect.Async
import cats.syntax.all._

import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}
import repcheck.ingestion.common.placeholders.{EntityRepository, PlaceholderCreator}
import repcheck.ingestion.votes.errors.BillResolutionFailed
import repcheck.shared.models.congress.dos.bill.BillDO

/**
 * Resolves bill natural keys (e.g., `"119-HR-1234"`) to internal `bills.id` Long PKs so votes with a legislation
 * reference can be persisted with a populated `votes.bill_id` FK.
 *
 * ==Flow==
 *   1. `PlaceholderCreator.ensureExists[BillDO]` performs an idempotent insert-if-not-exists on `bills` keyed by the
 *      natural key. The next scheduled run of `bill-metadata-pipeline` enriches the placeholder with the real bill
 *      metadata (title, sponsors, etc.). Votes-pipeline only needs the surrogate id, not the full record.
 *   2. `findBillIdByNaturalKey(naturalKey)` reads back the surrogate id. Supplied as a callback so votes-pipeline does
 *      not take a compile-time dependency on bills-common's repository trait — the processor wires this at
 *      construction as `nk => billRepo.findByBillId(nk).map(_.map(_.billId)).transact(xa)`.
 *
 * The resolver is only invoked when the incoming vote carries a `billNaturalKey`. Procedural votes (no legislation
 * reference) bypass this step entirely — `VoteDO.billId = None` is the correct domain representation of "vote not about
 * a specific bill."
 *
 * ==Why procedural votes still flow the full pipeline==
 * A procedural motion (cloture, motion to recommit, motion to suspend the rules, etc.) still has political signal —
 * members' positions on procedural votes feed party-loyalty metrics, alignment calculations, and other downstream
 * analyses. The votes-pipeline does not pre-filter by bill-linked-or-not; it records the vote + positions, emits
 * `VoteRecordedEvent` with `billNaturalKey = None`, and lets downstream consumers decide what to do. The only thing
 * that gates on `billId.isDefined` is the `stance_materialization_status.markHasVotes` call, because that table is
 * schema-keyed on `bill_id` and has no row shape for "procedural vote X."
 *
 * If `findBillIdByNaturalKey` returns `None` after `ensureExists` succeeded, the resolver raises
 * [[BillResolutionFailed]] as a per-vote failure — the stream keeps processing other votes.
 */
private[pipeline] class BillResolver[F[_]: Async](
  findBillIdByNaturalKey: String => F[Option[Long]],
  placeholderCreator: PlaceholderCreator[F],
  billEntityRepo: EntityRepository[F, BillDO],
  logger: PipelineLogger[F],
) {

  /**
   * Resolve a bill natural key to its Long PK. No-op when the caller passes `None` (procedural vote). Creates a
   * placeholder bill row if one does not already exist, then reads the surrogate id. Raises [[BillResolutionFailed]] on
   * the pathological "placeholder written but read came back empty" case.
   */
  def resolveOptional(maybeKey: Option[String], logCtx: LogContext): F[Option[Long]] =
    maybeKey match {
      case None     => Async[F].pure(None)
      case Some(nk) => resolve(nk, logCtx).map(Some(_))
    }

  /**
   * Resolve a single bill natural key to its Long PK. Caller guarantees `naturalKey` is non-empty; use
   * [[resolveOptional]] when the value may be absent.
   */
  def resolve(naturalKey: String, logCtx: LogContext): F[Long] =
    for {
      _          <- placeholderCreator.ensureExists[BillDO](naturalKey, billEntityRepo)
      maybeId    <- findBillIdByNaturalKey(naturalKey)
      resolvedId <- maybeId match {
        case Some(id) => Async[F].pure(id)
        case None =>
          val err = BillResolutionFailed(
            billNaturalKey = naturalKey,
            detail = "findBillIdByNaturalKey returned None after ensureExists — placeholder row disappeared",
          )
          logger.error(logCtx, err.getMessage, Some(err)) *> Async[F].raiseError[Long](err)
      }
    } yield resolvedId

}
