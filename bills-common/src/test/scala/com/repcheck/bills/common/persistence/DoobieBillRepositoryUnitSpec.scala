package com.repcheck.bills.common.persistence

import java.time.Instant

import cats.effect.IO
import cats.effect.unsafe.implicits.global

import doobie.Transactor

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import repcheck.shared.models.congress.common.BillType
import repcheck.shared.models.congress.dos.bill.BillDO

import com.repcheck.bills.common.errors.BillUpsertFailed

class DoobieBillRepositoryUnitSpec extends AnyFlatSpec with Matchers {

  private val failXa: Transactor[IO] = Transactor.fromDriverManager[IO](
    driver = "org.postgresql.Driver",
    url = "jdbc:postgresql://127.0.0.1:1/test?connectTimeout=1",
    user = "test",
    password = "test",
    logHandler = None,
  )

  private val repo = new DoobieBillRepository[IO](failXa)

  private val sampleBill = BillDO(
    billId = 0L,
    naturalKey = "118-HR-100",
    congress = 118,
    billType = BillType.HR,
    number = "100",
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
    updateDate = Some(Instant.parse("2024-01-01T00:00:00Z")),
    updateDateIncludingText = None,
    legislationUrl = None,
    apiUrl = None,
    createdAt = None,
    updatedAt = None,
    latestTextVersionId = None,
  )

  "upsert" should "wrap connection errors in BillUpsertFailed" in {
    val ex = intercept[BillUpsertFailed] {
      repo.upsert(sampleBill).unsafeRunSync()
    }
    ex.billId shouldBe "118-HR-100"
  }

  "findByBillId" should "propagate connection errors" in {
    assertThrows[Exception] {
      repo.findByBillId("118-HR-100").unsafeRunSync()
    }
  }

  "findByBillIds" should "propagate connection errors for non-empty list" in {
    assertThrows[Exception] {
      repo.findByBillIds(List("118-HR-100")).unsafeRunSync()
    }
  }

  "findBillsNeedingTextCheck" should "propagate connection errors" in {
    assertThrows[Exception] {
      repo.findBillsNeedingTextCheck().unsafeRunSync()
    }
  }

  "updateTextFields" should "propagate connection errors" in {
    assertThrows[Exception] {
      repo.updateTextFields("118-HR-100", "http://example.com", "XML", "IH", "2024-01-01T00:00:00Z", 1L).unsafeRunSync()
    }
  }

}
