package com.repcheck.bills.common.persistence

import cats.effect.IO
import cats.effect.unsafe.implicits.global

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import repcheck.shared.models.congress.dos.bill.{BillCosponsorDO, BillDO}

import com.repcheck.bills.common.testing.{DockerRequired, TransactorFixture}

class DoobieBillCosponsorRepositorySpec extends AnyFlatSpec with Matchers with TransactorFixture {

  private lazy val billRepo      = new DoobieBillRepository[IO](xa)
  private lazy val cosponsorRepo = new DoobieBillCosponsorRepository[IO](xa)

  private def insertBillAndGetId(): Long = {
    val bill = BillDO(
      billId = 0L,
      naturalKey = "118-HR-100",
      congress = 118,
      billType = "hr",
      number = "100",
      title = "Cosponsor Test Bill",
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
    val found = billRepo.findByBillId("118-HR-100").unsafeRunSync()
    found match {
      case Some(bill) => bill.billId
      case None       => sys.error("Expected bill to be present after insert")
    }
  }

  private def makeCosponsor(billId: Long, memberId: Long): BillCosponsorDO =
    BillCosponsorDO(
      billId = billId,
      memberId = memberId,
      isOriginalCosponsor = Some(true),
      sponsorshipDate = Some("2024-01-15"),
    )

  "replaceAll" should "insert cosponsors for a new bill" taggedAs DockerRequired in {
    val billId     = insertBillAndGetId()
    val cosponsors = List(makeCosponsor(billId, 1L), makeCosponsor(billId, 2L), makeCosponsor(billId, 3L))

    cosponsorRepo.replaceAll(billId, cosponsors).unsafeRunSync()
    val found = cosponsorRepo.findByBillId(billId).unsafeRunSync()
    found.size shouldBe 3
  }

  it should "replace cosponsors on re-ingest" taggedAs DockerRequired in {
    val billId = insertBillAndGetId()
    cosponsorRepo
      .replaceAll(billId, List(makeCosponsor(billId, 1L), makeCosponsor(billId, 2L), makeCosponsor(billId, 3L)))
      .unsafeRunSync()

    cosponsorRepo
      .replaceAll(billId, List(makeCosponsor(billId, 4L), makeCosponsor(billId, 5L)))
      .unsafeRunSync()

    val found = cosponsorRepo.findByBillId(billId).unsafeRunSync()
    found.size shouldBe 2
  }

  it should "handle empty list (clears all)" taggedAs DockerRequired in {
    val billId = insertBillAndGetId()
    cosponsorRepo.replaceAll(billId, List(makeCosponsor(billId, 1L))).unsafeRunSync()

    cosponsorRepo.replaceAll(billId, List.empty).unsafeRunSync()

    val found = cosponsorRepo.findByBillId(billId).unsafeRunSync()
    found shouldBe empty
  }

  "findByBillId" should "return empty for unknown bill" taggedAs DockerRequired in {
    val found = cosponsorRepo.findByBillId(99999L).unsafeRunSync()
    found shouldBe empty
  }

}
