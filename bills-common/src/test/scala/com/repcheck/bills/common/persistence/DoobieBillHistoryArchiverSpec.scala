package com.repcheck.bills.common.persistence

import cats.effect.IO
import cats.effect.unsafe.implicits.global

import doobie.implicits._

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import repcheck.shared.models.congress.dos.bill.{BillCosponsorDO, BillDO, BillSubjectDO}

import com.repcheck.bills.common.testing.{DockerRequired, TransactorFixture}

class DoobieBillHistoryArchiverSpec extends AnyFlatSpec with Matchers with TransactorFixture {

  private lazy val billRepo      = new DoobieBillRepository[IO](xa)
  private lazy val cosponsorRepo = new DoobieBillCosponsorRepository[IO](xa)
  private lazy val subjectRepo   = new DoobieBillSubjectRepository[IO](xa)
  private lazy val archiver      = new DoobieBillHistoryArchiver[IO](xa)

  private def insertFullBill(): Long = {
    val bill = BillDO(
      billId = 0L,
      naturalKey = "118-HR-300",
      congress = 118,
      billType = "hr",
      number = "300",
      title = "History Test Bill",
      originChamber = Some("House"),
      originChamberCode = Some("H"),
      introducedDate = Some("2024-01-10"),
      policyArea = Some("Health"),
      latestActionDate = Some("2024-03-15"),
      latestActionText = Some("Passed House"),
      constitutionalAuthorityText = None,
      sponsorMemberId = None,
      textUrl = None,
      textFormat = None,
      textVersionType = None,
      textDate = None,
      textContent = None,
      textEmbedding = None,
      summaryText = Some("A bill about health"),
      summaryActionDesc = None,
      summaryActionDate = None,
      updateDate = Some("2024-01-01T00:00:00Z"),
      updateDateIncludingText = None,
      legislationUrl = None,
      apiUrl = None,
      createdAt = None,
      updatedAt = None,
      latestTextVersionId = None,
    )
    billRepo.upsert(bill).unsafeRunSync()
    val billId = billRepo.findByBillId("118-HR-300").unsafeRunSync() match {
      case Some(b) => b.billId
      case None    => sys.error("Expected bill to be present after insert")
    }

    cosponsorRepo
      .replaceAll(
        billId,
        List(
          BillCosponsorDO(billId, 1L, Some(true), Some("2024-01-15")),
          BillCosponsorDO(billId, 2L, Some(false), Some("2024-02-01")),
          BillCosponsorDO(billId, 3L, Some(true), Some("2024-01-20")),
        ),
      )
      .unsafeRunSync()

    subjectRepo
      .replaceAll(
        billId,
        List(
          BillSubjectDO(billId, "Health", None, None),
          BillSubjectDO(billId, "Medicare", None, None),
          BillSubjectDO(billId, "Insurance", None, None),
          BillSubjectDO(billId, "Prescription drugs", None, None),
          BillSubjectDO(billId, "Public health", None, None),
        ),
      )
      .unsafeRunSync()

    billId
  }

  "archiveBill" should "copy bill row to bill_history" taggedAs DockerRequired in {
    val _         = insertFullBill()
    val historyId = archiver.archiveBill("118-HR-300").unsafeRunSync()

    val count = sql"SELECT COUNT(*) FROM bill_history WHERE id = $historyId"
      .query[Long]
      .unique
      .transact(xa)
      .unsafeRunSync()
    count shouldBe 1L
  }

  it should "copy cosponsors to bill_cosponsor_history" taggedAs DockerRequired in {
    val _         = insertFullBill()
    val historyId = archiver.archiveBill("118-HR-300").unsafeRunSync()

    val count = sql"SELECT COUNT(*) FROM bill_cosponsor_history WHERE history_id = $historyId"
      .query[Long]
      .unique
      .transact(xa)
      .unsafeRunSync()
    count shouldBe 3L
  }

  it should "copy subjects to bill_subject_history" taggedAs DockerRequired in {
    val _         = insertFullBill()
    val historyId = archiver.archiveBill("118-HR-300").unsafeRunSync()

    val count = sql"SELECT COUNT(*) FROM bill_subject_history WHERE history_id = $historyId"
      .query[Long]
      .unique
      .transact(xa)
      .unsafeRunSync()
    count shouldBe 5L
  }

  it should "link all history rows with the same history_id" taggedAs DockerRequired in {
    val _         = insertFullBill()
    val historyId = archiver.archiveBill("118-HR-300").unsafeRunSync()

    val billHistoryIds = sql"SELECT id FROM bill_history WHERE id = $historyId"
      .query[Long]
      .to[List]
      .transact(xa)
      .unsafeRunSync()
    val cospHistoryIds = sql"SELECT DISTINCT history_id FROM bill_cosponsor_history WHERE history_id = $historyId"
      .query[Long]
      .to[List]
      .transact(xa)
      .unsafeRunSync()
    val subjHistoryIds = sql"SELECT DISTINCT history_id FROM bill_subject_history WHERE history_id = $historyId"
      .query[Long]
      .to[List]
      .transact(xa)
      .unsafeRunSync()

    val _ = billHistoryIds should contain only historyId
    val _ = cospHistoryIds should contain only historyId
    subjHistoryIds should contain only historyId
  }

  it should "produce distinct history_id values for multiple archives" taggedAs DockerRequired in {
    val _   = insertFullBill()
    val id1 = archiver.archiveBill("118-HR-300").unsafeRunSync()
    val id2 = archiver.archiveBill("118-HR-300").unsafeRunSync()
    id1 should not be id2
  }

  it should "archive bill with no cosponsors or subjects" taggedAs DockerRequired in {
    val bill = BillDO(
      billId = 0L,
      naturalKey = "118-HR-301",
      congress = 118,
      billType = "hr",
      number = "301",
      title = "Bare Bill",
      originChamber = None,
      originChamberCode = None,
      introducedDate = None,
      policyArea = None,
      latestActionDate = None,
      latestActionText = None,
      constitutionalAuthorityText = None,
      sponsorMemberId = None,
      textUrl = None,
      textFormat = None,
      textVersionType = None,
      textDate = None,
      textContent = None,
      textEmbedding = None,
      summaryText = None,
      summaryActionDesc = None,
      summaryActionDate = None,
      updateDate = Some("2024-01-01T00:00:00Z"),
      updateDateIncludingText = None,
      legislationUrl = None,
      apiUrl = None,
      createdAt = None,
      updatedAt = None,
      latestTextVersionId = None,
    )
    billRepo.upsert(bill).unsafeRunSync()
    val historyId = archiver.archiveBill("118-HR-301").unsafeRunSync()

    val billCount = sql"SELECT COUNT(*) FROM bill_history WHERE id = $historyId"
      .query[Long]
      .unique
      .transact(xa)
      .unsafeRunSync()
    val cospCount = sql"SELECT COUNT(*) FROM bill_cosponsor_history WHERE history_id = $historyId"
      .query[Long]
      .unique
      .transact(xa)
      .unsafeRunSync()
    val subjCount = sql"SELECT COUNT(*) FROM bill_subject_history WHERE history_id = $historyId"
      .query[Long]
      .unique
      .transact(xa)
      .unsafeRunSync()

    val _ = billCount shouldBe 1L
    val _ = cospCount shouldBe 0L
    subjCount shouldBe 0L
  }

}
