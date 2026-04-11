package com.repcheck.bills.metadata.pipeline

import cats.Monad
import cats.effect.Async
import cats.syntax.all._

import doobie._
import doobie.implicits._

import repcheck.shared.models.congress.dos.bill.{BillCosponsorDO, BillDO, BillSubjectDO}

import com.repcheck.bills.common.persistence.{
  BillCosponsorRepository,
  BillHistoryArchiver,
  BillRepository,
  BillSubjectRepository,
  TransactionRunner,
}

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
    } yield ()

    TransactionRunner.run(xa)(writeProgram)
  }

}
