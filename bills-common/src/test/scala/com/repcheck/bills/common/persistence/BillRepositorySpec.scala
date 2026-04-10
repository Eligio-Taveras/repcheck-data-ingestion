package com.repcheck.bills.common.persistence

import cats.effect.IO
import cats.effect.unsafe.implicits.global

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import repcheck.shared.models.congress.dos.bill.BillDO

class BillRepositorySpec extends AnyFlatSpec with Matchers {

  private val stubRepo: BillRepository[IO] = new BillRepository[IO] {

    override def upsert(bill: BillDO): IO[Unit] = IO.unit

    override def findByBillId(billId: String): IO[Option[BillDO]] = IO.pure(None)

    override def findByBillIds(billIds: List[String]): IO[List[BillDO]] = IO.pure(List.empty)

    override def findBillsNeedingTextCheck(): IO[List[BillDO]] = IO.pure(List.empty)

    override def updateTextFields(
      billId: String,
      textUrl: String,
      textFormat: String,
      textVersionType: String,
      textDate: String,
      latestTextVersionId: Long,
    ): IO[Unit] = IO.unit
  }

  "BillRepository" should "compile with a stub implementation" in {
    stubRepo.toString should not be empty
  }

  it should "return Unit from upsert" in {
    val bill = BillDO(
      billId = 0L,
      naturalKey = "118-HR-1",
      congress = 118,
      billType = "hr",
      number = "1",
      title = "Test Bill",
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
      updateDate = None,
      updateDateIncludingText = None,
      legislationUrl = None,
      apiUrl = None,
      createdAt = None,
      updatedAt = None,
      latestTextVersionId = None,
    )
    stubRepo.upsert(bill).unsafeRunSync() shouldBe (())
  }

  it should "return None from findByBillId for a stub" in {
    stubRepo.findByBillId("118-HR-1").unsafeRunSync() shouldBe None
  }

  it should "return empty list from findByBillIds for a stub" in {
    stubRepo.findByBillIds(List("118-HR-1")).unsafeRunSync() shouldBe List.empty
  }

  it should "return empty list from findBillsNeedingTextCheck for a stub" in {
    stubRepo.findBillsNeedingTextCheck().unsafeRunSync() shouldBe List.empty
  }

  it should "return Unit from updateTextFields for a stub" in {
    stubRepo
      .updateTextFields("118-HR-1", "https://example.com", "xml", "Introduced", "2024-01-01", 1L)
      .unsafeRunSync() shouldBe (())
  }

}
