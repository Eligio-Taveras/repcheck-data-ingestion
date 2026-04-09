package com.repcheck.bills.common.persistence

import cats.effect.IO
import cats.effect.unsafe.implicits.global

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import repcheck.shared.models.congress.dos.bill.BillDO

import com.repcheck.bills.common.testing.{DockerRequired, TransactorFixture}

class DoobieBillRepositorySpec extends AnyFlatSpec with Matchers with TransactorFixture {

  private lazy val repo = new DoobieBillRepository[IO](xa)

  private def makeBill(
    congress: Int = 118,
    billType: String = "hr",
    number: String = "1234",
    title: String = "Test Bill",
    updateDate: Option[String] = Some("2024-01-15T00:00:00Z"),
  ): BillDO = BillDO(
    billId = 0L,
    naturalKey = s"$congress-${billType.toUpperCase}-$number",
    congress = congress,
    billType = billType,
    number = number,
    title = title,
    originChamber = Some("House"),
    originChamberCode = Some("H"),
    introducedDate = Some("2024-01-10"),
    policyArea = Some("Government Operations"),
    latestActionDate = Some("2024-03-15"),
    latestActionText = Some("Referred to committee"),
    constitutionalAuthorityText = Some("Article I"),
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
    updateDate = updateDate,
    updateDateIncludingText = None,
    legislationUrl = Some("https://congress.gov/bill/118th-congress/house-bill/1234"),
    apiUrl = Some("https://api.congress.gov/v3/bill/118/hr/1234"),
    createdAt = None,
    updatedAt = None,
    latestTextVersionId = None,
  )

  "upsert" should "insert a new bill" taggedAs DockerRequired in {
    val bill = makeBill()
    repo.upsert(bill).unsafeRunSync()

    val found = repo.findByBillId("118-HR-1234").unsafeRunSync()
    val _     = found shouldBe defined
    found match {
      case Some(bill) =>
        val _ = bill.title shouldBe "Test Bill"
        val _ = bill.congress shouldBe 118
        val _ = bill.billType shouldBe "hr"
        bill.naturalKey shouldBe "118-HR-1234"
      case None => fail("Expected bill to be present")
    }
  }

  it should "update an existing bill on conflict" taggedAs DockerRequired in {
    val bill = makeBill()
    repo.upsert(bill).unsafeRunSync()

    val updated = makeBill(title = "Updated Title")
    repo.upsert(updated).unsafeRunSync()

    val found = repo.findByBillId("118-HR-1234").unsafeRunSync()
    found.map(_.title) shouldBe Some("Updated Title")
  }

  it should "not create duplicate rows on conflict" taggedAs DockerRequired in {
    val bill = makeBill()
    repo.upsert(bill).unsafeRunSync()
    repo.upsert(bill).unsafeRunSync()

    val all = repo.findByBillIds(List("118-HR-1234")).unsafeRunSync()
    all.size shouldBe 1
  }

  it should "set updated_at on conflict" taggedAs DockerRequired in {
    val bill = makeBill()
    repo.upsert(bill).unsafeRunSync()
    val first = repo.findByBillId("118-HR-1234").unsafeRunSync()

    Thread.sleep(50)
    repo.upsert(makeBill(title = "Changed")).unsafeRunSync()
    val second = repo.findByBillId("118-HR-1234").unsafeRunSync()

    second.flatMap(_.updatedAt) should not be first.flatMap(_.updatedAt)
  }

  "findByBillId" should "return None for missing bill" taggedAs DockerRequired in {
    val found = repo.findByBillId("118-HR-9999").unsafeRunSync()
    found shouldBe None
  }

  it should "return stored bill with all fields" taggedAs DockerRequired in {
    val bill = makeBill()
    repo.upsert(bill).unsafeRunSync()

    val found = repo.findByBillId("118-HR-1234").unsafeRunSync()
    found match {
      case Some(bill) =>
        val _ = bill.originChamber shouldBe Some("House")
        val _ = bill.policyArea shouldBe Some("Government Operations")
        bill.legislationUrl shouldBe Some("https://congress.gov/bill/118th-congress/house-bill/1234")
      case None => fail("Expected bill to be present")
    }
  }

  "findByBillIds" should "return matching bills" taggedAs DockerRequired in {
    repo.upsert(makeBill(number = "1")).unsafeRunSync()
    repo.upsert(makeBill(number = "2")).unsafeRunSync()
    repo.upsert(makeBill(number = "3")).unsafeRunSync()

    val found = repo.findByBillIds(List("118-HR-1", "118-HR-2")).unsafeRunSync()
    found.size shouldBe 2
  }

  it should "ignore missing IDs" taggedAs DockerRequired in {
    repo.upsert(makeBill(number = "1")).unsafeRunSync()

    val found = repo.findByBillIds(List("118-HR-1", "118-HR-9999")).unsafeRunSync()
    found.size shouldBe 1
  }

  it should "return empty for empty input" taggedAs DockerRequired in {
    val found = repo.findByBillIds(List.empty).unsafeRunSync()
    found shouldBe empty
  }

  "findBillsNeedingTextCheck" should "return bills without text" taggedAs DockerRequired in {
    repo.upsert(makeBill(number = "1")).unsafeRunSync()
    val found = repo.findBillsNeedingTextCheck().unsafeRunSync()
    found.map(_.naturalKey) should contain("118-HR-1")
  }

  it should "include non-final text bills" taggedAs DockerRequired in {
    val bill = makeBill(number = "2")
    repo.upsert(bill).unsafeRunSync()

    repo
      .updateTextFields("118-HR-2", "http://text", "Formatted Text", "IH", "2024-01-15T00:00:00Z", 1L)
      .unsafeRunSync()

    val found = repo.findBillsNeedingTextCheck().unsafeRunSync()
    found.map(_.naturalKey) should contain("118-HR-2")
  }

  "updateTextFields" should "set text columns without touching metadata" taggedAs DockerRequired in {
    repo.upsert(makeBill()).unsafeRunSync()
    val versionId = 42L

    repo
      .updateTextFields("118-HR-1234", "http://text.xml", "Formatted XML", "RH", "2024-02-01T00:00:00Z", versionId)
      .unsafeRunSync()

    val found = repo.findByBillId("118-HR-1234").unsafeRunSync()
    found match {
      case Some(bill) =>
        val _ = bill.title shouldBe "Test Bill"
        val _ = bill.textUrl shouldBe Some("http://text.xml")
        val _ = bill.textFormat shouldBe Some("Formatted XML")
        val _ = bill.textVersionType shouldBe Some("RH")
        bill.latestTextVersionId shouldBe Some(versionId)
      case None => fail("Expected bill to be present")
    }
  }

}
