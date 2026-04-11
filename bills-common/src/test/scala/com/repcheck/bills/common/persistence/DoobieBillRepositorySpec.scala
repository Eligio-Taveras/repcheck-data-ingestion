package com.repcheck.bills.common.persistence

import java.time.{Instant, LocalDate}

import cats.effect.IO
import cats.effect.unsafe.implicits.global

import doobie.implicits._

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import repcheck.shared.models.congress.bill.TextVersionCode
import repcheck.shared.models.congress.common.{BillType, Chamber, FormatType}
import repcheck.shared.models.congress.dos.bill.{BillDO, BillTextVersionDO}

import com.repcheck.bills.common.testing.{DockerRequired, TransactorFixture}

class DoobieBillRepositorySpec extends AnyFlatSpec with Matchers with TransactorFixture {

  private lazy val repo            = new DoobieBillRepository
  private lazy val textVersionRepo = new DoobieBillTextVersionRepository

  /** Insert a text version row and return its auto-generated id for FK-safe updateTextFields tests. */
  private def insertTextVersion(billId: Long): Long =
    textVersionRepo
      .insertVersion(
        BillTextVersionDO(
          id = 0L,
          billId = billId,
          versionCode = "IH",
          versionType = "IH version",
          versionDate = Some(LocalDate.parse("2024-01-15")),
          formatType = Some(FormatType.FormattedText),
          url = Some("https://congress.gov/text/IH"),
          content = None,
          embedding = None,
          fetchedAt = Some(Instant.now()),
          createdAt = None,
        )
      )
      .transact(xa)
      .unsafeRunSync()

  private def makeBill(
    congress: Int = 118,
    billType: BillType = BillType.HR,
    number: String = "1234",
    title: String = "Test Bill",
    updateDate: Option[Instant] = Some(Instant.parse("2024-01-15T00:00:00Z")),
  ): BillDO = BillDO(
    billId = 0L,
    naturalKey = s"$congress-${billType.toString}-$number",
    congress = congress,
    billType = billType,
    number = number,
    title = title,
    originChamber = Some(Chamber.House),
    originChamberCode = Some("H"),
    introducedDate = Some(LocalDate.parse("2024-01-10")),
    policyArea = Some("Government Operations"),
    latestActionDate = Some(LocalDate.parse("2024-03-15")),
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
    val _    = repo.upsert(bill).transact(xa).unsafeRunSync()

    val found = repo.findByBillId("118-HR-1234").transact(xa).unsafeRunSync()
    val _     = found shouldBe defined
    found match {
      case Some(bill) =>
        val _ = bill.title shouldBe "Test Bill"
        val _ = bill.congress shouldBe 118
        val _ = bill.billType shouldBe BillType.HR
        bill.naturalKey shouldBe "118-HR-1234"
      case None => fail("Expected bill to be present")
    }
  }

  it should "return the generated bill id" taggedAs DockerRequired in {
    val bill       = makeBill()
    val returnedId = repo.upsert(bill).transact(xa).unsafeRunSync()
    returnedId should be > 0L
  }

  it should "update an existing bill on conflict" taggedAs DockerRequired in {
    val bill = makeBill()
    val _    = repo.upsert(bill).transact(xa).unsafeRunSync()

    val updated = makeBill(title = "Updated Title")
    val _       = repo.upsert(updated).transact(xa).unsafeRunSync()

    val found = repo.findByBillId("118-HR-1234").transact(xa).unsafeRunSync()
    found.map(_.title) shouldBe Some("Updated Title")
  }

  it should "not create duplicate rows on conflict" taggedAs DockerRequired in {
    val bill = makeBill()
    val _    = repo.upsert(bill).transact(xa).unsafeRunSync()
    val _    = repo.upsert(bill).transact(xa).unsafeRunSync()

    val all = repo.findByBillIds(List("118-HR-1234")).transact(xa).unsafeRunSync()
    all.size shouldBe 1
  }

  it should "set updated_at on conflict" taggedAs DockerRequired in {
    val bill  = makeBill()
    val _     = repo.upsert(bill).transact(xa).unsafeRunSync()
    val first = repo.findByBillId("118-HR-1234").transact(xa).unsafeRunSync()

    Thread.sleep(50)
    val _      = repo.upsert(makeBill(title = "Changed")).transact(xa).unsafeRunSync()
    val second = repo.findByBillId("118-HR-1234").transact(xa).unsafeRunSync()

    second.flatMap(_.updatedAt) should not be first.flatMap(_.updatedAt)
  }

  "findByBillId" should "return None for missing bill" taggedAs DockerRequired in {
    val found = repo.findByBillId("118-HR-9999").transact(xa).unsafeRunSync()
    found shouldBe None
  }

  it should "return stored bill with all fields" taggedAs DockerRequired in {
    val bill = makeBill()
    val _    = repo.upsert(bill).transact(xa).unsafeRunSync()

    val found = repo.findByBillId("118-HR-1234").transact(xa).unsafeRunSync()
    found match {
      case Some(bill) =>
        val _ = bill.originChamber shouldBe Some(Chamber.House)
        val _ = bill.policyArea shouldBe Some("Government Operations")
        bill.legislationUrl shouldBe Some("https://congress.gov/bill/118th-congress/house-bill/1234")
      case None => fail("Expected bill to be present")
    }
  }

  "findByBillIds" should "return matching bills" taggedAs DockerRequired in {
    val _ = repo.upsert(makeBill(number = "1")).transact(xa).unsafeRunSync()
    val _ = repo.upsert(makeBill(number = "2")).transact(xa).unsafeRunSync()
    val _ = repo.upsert(makeBill(number = "3")).transact(xa).unsafeRunSync()

    val found = repo.findByBillIds(List("118-HR-1", "118-HR-2")).transact(xa).unsafeRunSync()
    found.size shouldBe 2
  }

  it should "ignore missing IDs" taggedAs DockerRequired in {
    val _ = repo.upsert(makeBill(number = "1")).transact(xa).unsafeRunSync()

    val found = repo.findByBillIds(List("118-HR-1", "118-HR-9999")).transact(xa).unsafeRunSync()
    found.size shouldBe 1
  }

  it should "return empty for empty input" taggedAs DockerRequired in {
    val found = repo.findByBillIds(List.empty).transact(xa).unsafeRunSync()
    found shouldBe empty
  }

  "findBillsNeedingTextCheck" should "return bills without text" taggedAs DockerRequired in {
    val _     = repo.upsert(makeBill(number = "1")).transact(xa).unsafeRunSync()
    val found = repo.findBillsNeedingTextCheck().transact(xa).unsafeRunSync()
    found.map(_.naturalKey) should contain("118-HR-1")
  }

  it should "include non-final text bills" taggedAs DockerRequired in {
    val bill = makeBill(number = "2")
    val _    = repo.upsert(bill).transact(xa).unsafeRunSync()
    val billId =
      repo.findByBillId("118-HR-2").transact(xa).unsafeRunSync().map(_.billId).getOrElse(sys.error("missing"))
    val versionId = insertTextVersion(billId)

    repo
      .updateTextFields("118-HR-2", "http://text", "Formatted Text", "IH", "2024-01-15T00:00:00Z", versionId)
      .transact(xa)
      .unsafeRunSync()

    val found = repo.findBillsNeedingTextCheck().transact(xa).unsafeRunSync()
    found.map(_.naturalKey) should contain("118-HR-2")
  }

  "updateTextFields" should "set text columns without touching metadata" taggedAs DockerRequired in {
    val _ = repo.upsert(makeBill()).transact(xa).unsafeRunSync()
    val billId =
      repo.findByBillId("118-HR-1234").transact(xa).unsafeRunSync().map(_.billId).getOrElse(sys.error("missing"))
    val versionId = insertTextVersion(billId)

    repo
      .updateTextFields("118-HR-1234", "http://text.xml", "Formatted XML", "RH", "2024-02-01T00:00:00Z", versionId)
      .transact(xa)
      .unsafeRunSync()

    val found = repo.findByBillId("118-HR-1234").transact(xa).unsafeRunSync()
    found match {
      case Some(bill) =>
        val _ = bill.title shouldBe "Test Bill"
        val _ = bill.textUrl shouldBe Some("http://text.xml")
        val _ = bill.textFormat shouldBe Some(FormatType.FormattedXml)
        val _ = bill.textVersionType shouldBe Some(TextVersionCode.RH)
        bill.latestTextVersionId shouldBe Some(versionId)
      case None => fail("Expected bill to be present")
    }
  }

}
