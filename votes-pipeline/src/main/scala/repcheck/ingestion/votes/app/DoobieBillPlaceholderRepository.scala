package repcheck.ingestion.votes.app

import cats.effect.kernel.MonadCancelThrow
import cats.syntax.all._

import doobie.implicits._
import doobie.util.transactor.Transactor

import repcheck.ingestion.common.placeholders.EntityRepository
import repcheck.ingestion.votes.errors.BillResolutionFailed
import repcheck.shared.models.congress.common.BillType
import repcheck.shared.models.congress.common.DoobieEnumInstances._
import repcheck.shared.models.congress.dos.bill.BillDO

/**
 * Doobie-backed [[EntityRepository]] for [[BillDO]] placeholder inserts. The `bills` table has no `natural_key` column
 * — its uniqueness constraint is on the composite `(congress, bill_type, number)` — so we can't reuse the generic
 * `DoobieEntityRepository` pattern (which assumes a single surrogate key and a trivial `HasPlaceholder` mapping).
 *
 * This repository parses the Scala-side natural key (e.g., `"119-HR-30"`) into its three composite fields, inserts with
 * `ON CONFLICT (congress, bill_type, number) DO NOTHING` to stay idempotent, and delegates to the bills-pipeline to
 * enrich the row on its next scheduled run.
 *
 * ==Parse contract==
 *
 * The natural key must be in the form `"<congress>-<BILL_TYPE>-<number>"` as emitted by
 * `repcheck.shared.models.congress.dto.conversions.BillConversions.buildBillNaturalKey`. Any deviation (wrong segment
 * count, non-numeric congress, unknown bill type, non-numeric number) raises [[BillResolutionFailed]] so the caller's
 * per-vote failure isolation records `ProcessingResult.Failed` without corrupting the table.
 *
 * ==What gets inserted==
 *
 *   - `congress`, `bill_type`, `number` — parsed from the natural key.
 *   - `title` — empty string (the downstream bills-pipeline overwrites it with the real value).
 *   - `update_date` — NOW() (required NOT NULL; the real update timestamp is written on enrichment).
 *
 * All other columns keep their DB defaults (NULL / NOW()).
 */
class DoobieBillPlaceholderRepository[F[_]: MonadCancelThrow](xa: Transactor[F]) extends EntityRepository[F, BillDO] {

  override def insertIfNotExists(entity: BillDO): F[Unit] =
    parseNaturalKey(entity.naturalKey) match {
      case Right((congress, billType, number)) =>
        sql"""INSERT INTO bills (congress, bill_type, number, title, update_date)
              VALUES ($congress, $billType, $number, '', NOW())
              ON CONFLICT (congress, bill_type, number) DO NOTHING""".update.run.transact(xa).void

      case Left(err) =>
        MonadCancelThrow[F].raiseError[Unit](err)
    }

  /**
   * Parse `"<congress>-<BILL_TYPE>-<number>"` into `(Int, BillType, Int)`. Returns [[BillResolutionFailed]] on any
   * parse failure so failures surface as a typed, project-owned error.
   */
  private[app] def parseNaturalKey(nk: String): Either[BillResolutionFailed, (Int, BillType, Int)] = {
    val parts = nk.split("-", 3)
    if (parts.length != 3) {
      Left(BillResolutionFailed(nk, s"natural key must have 3 '-' segments; got ${parts.length.toString}"))
    } else {
      val parsed = for {
        congress <- parts(0).toIntOption.toRight(s"congress segment '${parts(0)}' is not an int")
        billType <- BillType.fromString(parts(1)).left.map(_.getMessage)
        number   <- parts(2).toIntOption.toRight(s"number segment '${parts(2)}' is not an int")
      } yield (congress, billType, number)
      parsed.left.map(reason => BillResolutionFailed(nk, reason))
    }
  }

}
