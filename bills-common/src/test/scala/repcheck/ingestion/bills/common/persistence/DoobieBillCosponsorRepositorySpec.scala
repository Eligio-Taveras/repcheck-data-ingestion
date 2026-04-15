package repcheck.ingestion.bills.common.persistence

import java.time.{Instant, LocalDate}

import cats.effect.IO
import cats.effect.unsafe.implicits.global

import doobie.implicits._

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import repcheck.ingestion.bills.common.testing.{DockerRequired, TransactorFixture}
import repcheck.shared.models.congress.common.BillType
import repcheck.shared.models.congress.dos.bill.{BillCosponsorDO, BillDO}

class DoobieBillCosponsorRepositorySpec extends AnyFlatSpec with Matchers with TransactorFixture {

  private lazy val billRepo      = new DoobieBillRepository
  private lazy val cosponsorRepo = new DoobieBillCosponsorRepository

  private def insertBillAndGetId(): Long = {
    val bill = BillDO(
      billId = 0L,
      naturalKey = "118-HR-100",
      congress = 118,
      billType = BillType.HR,
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
      summaryText = None,
      summaryActionDesc = None,
      summaryActionDate = None,
      updateDate = Some(Instant.parse("2024-01-01T00:00:00Z")),
      updateDateIncludingText = None,
      legislationUrl = None,
      apiUrl = None,
      createdAt = None,
      updatedAt = None,
      latestTextVersionId = None,
    )
    val _     = billRepo.upsert(bill).transact(xa).unsafeRunSync()
    val found = billRepo.findByBillId("118-HR-100").transact(xa).unsafeRunSync()
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
      sponsorshipDate = Some(LocalDate.parse("2024-01-15")),
    )

  "replaceAll" should "insert cosponsors for a new bill" taggedAs DockerRequired in {
    val billId     = insertBillAndGetId()
    val m1         = memberIdByKey("TEST-M001")
    val m2         = memberIdByKey("TEST-M002")
    val m3         = memberIdByKey("TEST-M003")
    val cosponsors = List(makeCosponsor(billId, m1), makeCosponsor(billId, m2), makeCosponsor(billId, m3))

    cosponsorRepo.replaceAll(billId, cosponsors).transact(xa).unsafeRunSync()
    val found = cosponsorRepo.findByBillId(billId).transact(xa).unsafeRunSync()
    found.size shouldBe 3
  }

  it should "replace cosponsors on re-ingest" taggedAs DockerRequired in {
    val billId = insertBillAndGetId()
    val m1     = memberIdByKey("TEST-M001")
    val m2     = memberIdByKey("TEST-M002")
    val m3     = memberIdByKey("TEST-M003")
    val m4     = memberIdByKey("TEST-M004")
    val m5     = memberIdByKey("TEST-M005")
    cosponsorRepo
      .replaceAll(billId, List(makeCosponsor(billId, m1), makeCosponsor(billId, m2), makeCosponsor(billId, m3)))
      .transact(xa)
      .unsafeRunSync()

    cosponsorRepo
      .replaceAll(billId, List(makeCosponsor(billId, m4), makeCosponsor(billId, m5)))
      .transact(xa)
      .unsafeRunSync()

    val found = cosponsorRepo.findByBillId(billId).transact(xa).unsafeRunSync()
    found.size shouldBe 2
  }

  it should "handle empty list (clears all)" taggedAs DockerRequired in {
    val billId = insertBillAndGetId()
    val m1     = memberIdByKey("TEST-M001")
    cosponsorRepo.replaceAll(billId, List(makeCosponsor(billId, m1))).transact(xa).unsafeRunSync()

    cosponsorRepo.replaceAll(billId, List.empty).transact(xa).unsafeRunSync()

    val found = cosponsorRepo.findByBillId(billId).transact(xa).unsafeRunSync()
    found shouldBe empty
  }

  "findByBillId" should "return empty for unknown bill" taggedAs DockerRequired in {
    val found = cosponsorRepo.findByBillId(99999L).transact(xa).unsafeRunSync()
    found shouldBe empty
  }

}
