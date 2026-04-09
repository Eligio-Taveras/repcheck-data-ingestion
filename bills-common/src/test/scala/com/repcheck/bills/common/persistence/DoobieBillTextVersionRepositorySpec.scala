package com.repcheck.bills.common.persistence

import java.time.Instant
import java.util.UUID

import cats.effect.IO
import cats.effect.unsafe.implicits.global

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import repcheck.shared.models.congress.dos.bill.{BillDO, BillTextVersionDO}

import com.repcheck.bills.common.testing.{DockerRequired, TransactorFixture}

class DoobieBillTextVersionRepositorySpec extends AnyFlatSpec with Matchers with TransactorFixture {

  private lazy val billRepo        = new DoobieBillRepository[IO](xa)
  private lazy val textVersionRepo = new DoobieBillTextVersionRepository[IO](xa)

  private def insertBillAndGetId(): Long = {
    val bill = BillDO(
      billId = 0L,
      naturalKey = "118-HR-400",
      congress = 118,
      billType = "hr",
      number = "400",
      title = "Text Version Test Bill",
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
    billRepo.findByBillId("118-HR-400").unsafeRunSync() match {
      case Some(bill) => bill.billId
      case None       => sys.error("Expected bill to be present after insert")
    }
  }

  private def makeVersion(billId: Long, code: String, date: String): BillTextVersionDO =
    BillTextVersionDO(
      versionId = UUID.randomUUID(),
      billId = billId,
      versionCode = code,
      versionType = s"$code version",
      versionDate = Some(date),
      formatType = Some("Formatted Text"),
      url = Some(s"https://congress.gov/text/$code"),
      content = Some(s"Full text content for $code version of the bill."),
      embedding = None,
      fetchedAt = Some(Instant.now()),
      createdAt = None,
    )

  "insertVersion" should "create row with generated UUID" taggedAs DockerRequired in {
    val billId  = insertBillAndGetId()
    val version = makeVersion(billId, "IH", "2024-01-15T00:00:00Z")

    val returnedId = textVersionRepo.insertVersion(version).unsafeRunSync()
    val _          = returnedId shouldBe version.versionId

    val found = textVersionRepo.findByBillId(billId).unsafeRunSync()
    val _     = found.size shouldBe 1
    found.headOption.map(_.versionId) shouldBe Some(returnedId)
  }

  it should "store all fields correctly" taggedAs DockerRequired in {
    val billId  = insertBillAndGetId()
    val version = makeVersion(billId, "RH", "2024-02-01T00:00:00Z")

    val _     = textVersionRepo.insertVersion(version).unsafeRunSync()
    val found = textVersionRepo.findByBillId(billId).unsafeRunSync()
    found.headOption match {
      case Some(v) =>
        val _ = v.versionCode shouldBe "RH"
        val _ = v.versionType shouldBe "RH version"
        val _ = v.formatType shouldBe Some("Formatted Text")
        v.url shouldBe Some("https://congress.gov/text/RH")
      case None => fail("Expected at least one text version")
    }
  }

  it should "preserve content text" taggedAs DockerRequired in {
    val billId      = insertBillAndGetId()
    val longContent = "A" * 10000
    val version     = makeVersion(billId, "IH", "2024-01-15T00:00:00Z").copy(content = Some(longContent))

    val _     = textVersionRepo.insertVersion(version).unsafeRunSync()
    val found = textVersionRepo.findByBillId(billId).unsafeRunSync()
    found.headOption.flatMap(_.content) shouldBe Some(longContent)
  }

  it should "allow multiple versions per bill (append-only)" taggedAs DockerRequired in {
    val billId = insertBillAndGetId()

    val _ = textVersionRepo.insertVersion(makeVersion(billId, "IH", "2024-01-15T00:00:00Z")).unsafeRunSync()
    val _ = textVersionRepo.insertVersion(makeVersion(billId, "RH", "2024-02-01T00:00:00Z")).unsafeRunSync()
    val _ = textVersionRepo.insertVersion(makeVersion(billId, "EH", "2024-03-01T00:00:00Z")).unsafeRunSync()

    val found = textVersionRepo.findByBillId(billId).unsafeRunSync()
    found.size shouldBe 3
  }

  "findByBillId" should "order by version_date descending" taggedAs DockerRequired in {
    val billId = insertBillAndGetId()

    val _ = textVersionRepo.insertVersion(makeVersion(billId, "IH", "2024-01-15T00:00:00Z")).unsafeRunSync()
    val _ = textVersionRepo.insertVersion(makeVersion(billId, "EH", "2024-03-01T00:00:00Z")).unsafeRunSync()
    val _ = textVersionRepo.insertVersion(makeVersion(billId, "RH", "2024-02-01T00:00:00Z")).unsafeRunSync()

    val found = textVersionRepo.findByBillId(billId).unsafeRunSync()
    found.map(_.versionCode) shouldBe List("EH", "RH", "IH")
  }

  it should "return empty for unknown bill" taggedAs DockerRequired in {
    val found = textVersionRepo.findByBillId(99999L).unsafeRunSync()
    found shouldBe empty
  }

  "findLatestByBillId" should "return most recent version" taggedAs DockerRequired in {
    val billId = insertBillAndGetId()

    val _ = textVersionRepo.insertVersion(makeVersion(billId, "IH", "2024-01-15T00:00:00Z")).unsafeRunSync()
    val _ = textVersionRepo.insertVersion(makeVersion(billId, "RH", "2024-02-01T00:00:00Z")).unsafeRunSync()
    val _ = textVersionRepo.insertVersion(makeVersion(billId, "EH", "2024-03-01T00:00:00Z")).unsafeRunSync()

    val latest = textVersionRepo.findLatestByBillId(billId).unsafeRunSync()
    val _      = latest shouldBe defined
    latest.map(_.versionCode) shouldBe Some("EH")
  }

  it should "return None for unknown bill" taggedAs DockerRequired in {
    val latest = textVersionRepo.findLatestByBillId(99999L).unsafeRunSync()
    latest shouldBe None
  }

  "storeAndUpdateBill" should "insert version AND update bill text fields" taggedAs DockerRequired in {
    val billId  = insertBillAndGetId()
    val version = makeVersion(billId, "ENR", "2024-04-01T00:00:00Z")

    val versionId = textVersionRepo.storeAndUpdateBill(version).unsafeRunSync()

    val versions = textVersionRepo.findByBillId(billId).unsafeRunSync()
    val _        = versions.size shouldBe 1
    val _        = versions.headOption.map(_.versionId) shouldBe Some(versionId)

    billRepo.findByBillId("118-HR-400").unsafeRunSync() match {
      case Some(bill) =>
        val _ = bill.textUrl shouldBe Some("https://congress.gov/text/ENR")
        val _ = bill.textFormat shouldBe Some("Formatted Text")
        val _ = bill.textVersionType shouldBe Some("ENR")
        bill.latestTextVersionId shouldBe Some(versionId)
      case None => fail("Expected bill to be present")
    }
  }

  it should "set latest_text_version_id on bill" taggedAs DockerRequired in {
    val billId  = insertBillAndGetId()
    val version = makeVersion(billId, "IH", "2024-01-15T00:00:00Z")

    val versionId = textVersionRepo.storeAndUpdateBill(version).unsafeRunSync()
    billRepo.findByBillId("118-HR-400").unsafeRunSync().map(_.latestTextVersionId) shouldBe Some(Some(versionId))
  }

  "embedding" should "be None at insert time" taggedAs DockerRequired in {
    val billId  = insertBillAndGetId()
    val version = makeVersion(billId, "IH", "2024-01-15T00:00:00Z")

    val _     = textVersionRepo.insertVersion(version).unsafeRunSync()
    val found = textVersionRepo.findByBillId(billId).unsafeRunSync()
    found.headOption.flatMap(_.embedding) shouldBe None
  }

}
