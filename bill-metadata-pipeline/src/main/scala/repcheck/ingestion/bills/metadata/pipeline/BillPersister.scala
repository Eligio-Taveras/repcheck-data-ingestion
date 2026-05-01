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
import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}
import repcheck.shared.models.congress.bill.TextVersionCode
import repcheck.shared.models.congress.common.Chamber
import repcheck.shared.models.congress.dos.bill.{BillCosponsorDO, BillDO, BillSubjectDO}

private[pipeline] class BillPersister[F[_]: Async](
  billRepo: BillRepository[ConnectionIO],
  cosponsorRepo: BillCosponsorRepository[ConnectionIO],
  subjectRepo: BillSubjectRepository[ConnectionIO],
  historyArchiver: BillHistoryArchiver[ConnectionIO],
  xa: Transactor[F],
  logger: PipelineLogger[F],
) {

  private val persistLogCtx = LogContext("0", "bill-metadata-persist")

  private[pipeline] def persistBill(
    billDO: BillDO,
    subjects: List[BillSubjectDO],
    cosponsorDOs: List[BillCosponsorDO],
    naturalKey: String,
    isNew: Boolean,
  ): F[Unit] = {
    val ctx = persistLogCtx.copy(entityId = Some(naturalKey))

    for {
      _ <- logger.debug(
        ctx,
        s"persistBill.start naturalKey=$naturalKey isNew=${isNew.toString} " +
          s"subjects=${subjects.size.toString} cosponsors=${cosponsorDOs.size.toString} " +
          s"originChamber=${billDO.originChamber.fold("none")(_.toString)} " +
          s"sponsorMemberId=${billDO.sponsorMemberId.fold("none")(_.toString)}",
      )

      writeProgram = for {
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
      } yield billId

      _      <- logger.debug(ctx, s"persistBill.txn.start naturalKey=$naturalKey")
      billId <- TransactionRunner.run(xa)(writeProgram)
      _      <- logger.debug(ctx, s"persistBill.txn.committed naturalKey=$naturalKey billId=${billId.toString}")
    } yield ()
  }

  private[pipeline] def floorFor(chamber: Option[Chamber]): Option[TextVersionCode] =
    chamber match {
      case Some(Chamber.House)  => Some(TextVersionCode.IH)
      case Some(Chamber.Senate) => Some(TextVersionCode.IS)
      case _                    => None
    }

  private[pipeline] def applyExpectedVersionFloor(
    billDO: BillDO,
    naturalKey: String,
  ): ConnectionIO[Unit] =
    floorFor(billDO.originChamber) match {
      case None => Monad[ConnectionIO].unit
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
