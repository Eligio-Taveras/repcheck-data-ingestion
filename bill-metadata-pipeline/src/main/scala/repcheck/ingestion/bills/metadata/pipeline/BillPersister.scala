package repcheck.ingestion.bills.metadata.pipeline

import cats.Monad
import cats.effect.Async
import cats.syntax.all._

import doobie._
import doobie.implicits._

import repcheck.ingestion.bills.common.persistence.{
  BillCosponsorRepository,
  BillHistoryArchiver,
  BillRepository,
  BillSubjectRepository,
  TransactionRunner,
}
import repcheck.shared.models.congress.bill.TextVersionCode
import repcheck.shared.models.congress.common.Chamber
import repcheck.shared.models.congress.dos.bill.{BillCosponsorDO, BillDO, BillSubjectDO}

/**
 * Composes archive-before-overwrite, upsert, cosponsor replace, and subject replace into a single atomic
 * [[ConnectionIO]] transaction via [[TransactionRunner]].
 */
private[pipeline] class BillPersister[F[_]: Async](
  billRepo: BillRepository[ConnectionIO],
  cosponsorRepo: BillCosponsorRepository[ConnectionIO],
  subjectRepo: BillSubjectRepository[ConnectionIO],
  historyArchiver: BillHistoryArchiver[ConnectionIO],
  xa: Transactor[F],
) {

  /**
   * Persists a bill and its related cosponsors and subjects in a single atomic transaction. When the bill is not new,
   * the current state is archived into history tables before the upsert.
   *
   * As part of the same transaction the bill's `expected_text_version_code` gets a baseline-floor write derived from
   * `originChamber` (House → IH, Senate → IS, Joint or unknown → no write). The floor is the minimum stage every bill
   * starts at — without it, bills CRS never summarizes (the majority of introduced bills) would have
   * `expected_text_version_code = NULL` forever and be excluded from the bill-text-availability-checker sweep. The
   * floor write is conditional via a `progressionOrder` regression guard: it skips when the bill is already at or past
   * the floor stage (advanced by `bill-summary-pipeline` or by a previous run's same-floor write).
   */
  private[pipeline] def persistBill(
    billDO: BillDO,
    subjects: List[BillSubjectDO],
    cosponsorDOs: List[BillCosponsorDO],
    naturalKey: String,
    isNew: Boolean,
  ): F[Unit] = {
    val writeProgram: ConnectionIO[Unit] = for {
      _ <-
        if (!isNew) {
          historyArchiver.archiveBill(naturalKey).void
        } else {
          Monad[ConnectionIO].unit
        }
      billId <- billRepo.upsert(billDO)
      _      <- cosponsorRepo.replaceAll(billId, cosponsorDOs.map(_.copy(billId = billId)))
      _      <- subjectRepo.replaceAll(billId, subjects.map(_.copy(billId = billId)))
      _      <- applyExpectedVersionFloor(billDO, naturalKey)
    } yield ()

    TransactionRunner.run(xa)(writeProgram)
  }

  /**
   * Maps `originChamber` to its corresponding introduced-stage [[TextVersionCode]] (House → IH, Senate → IS). Returns
   * `None` for `Chamber.Joint` (no legislative-text equivalent — joint resolutions originate in either chamber and
   * carry IH/IS depending on which) and for missing data; the regression guard below treats `None` as "no floor to
   * apply" and skips the write.
   */
  private[pipeline] def floorFor(chamber: Option[Chamber]): Option[TextVersionCode] =
    chamber match {
      case Some(Chamber.House)  => Some(TextVersionCode.IH)
      case Some(Chamber.Senate) => Some(TextVersionCode.IS)
      case _                    => None
    }

  /**
   * Read the current `expected_text_version_code` for the bill, compare against the introduced floor derived from
   * `originChamber`, and write the floor when the existing value is None or behind. Cooperates with
   * `bill-summary-pipeline`'s advancing writes via the same `progressionOrder`-based regression guard so neither
   * pipeline can downgrade the other's writes.
   */
  private[pipeline] def applyExpectedVersionFloor(
    billDO: BillDO,
    naturalKey: String,
  ): ConnectionIO[Unit] =
    floorFor(billDO.originChamber) match {
      case None => Monad[ConnectionIO].unit // unrecognized chamber or missing — leave column alone
      case Some(floor) =>
        billRepo.findExpectedVersion(naturalKey).flatMap { existing =>
          val shouldWrite = existing match {
            case None          => true
            case Some(current) => floor.progressionOrder > current.progressionOrder
          }
          if (shouldWrite) billRepo.updateExpectedVersion(naturalKey, floor)
          else Monad[ConnectionIO].unit
        }
    }

}
