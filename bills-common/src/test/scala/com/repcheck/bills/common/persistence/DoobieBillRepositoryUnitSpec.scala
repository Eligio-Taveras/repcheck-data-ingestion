package com.repcheck.bills.common.persistence

import java.time.Instant

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import repcheck.shared.models.congress.common.BillType
import repcheck.shared.models.congress.dos.bill.BillDO

class DoobieBillRepositoryUnitSpec extends AnyFlatSpec with Matchers {

  private val repo = new DoobieBillRepository

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

  "upsert" should "produce a ConnectionIO for the upsert" in {
    val cio = repo.upsert(sampleBill)
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  "findByBillId" should "produce a ConnectionIO for the query" in {
    val cio = repo.findByBillId("118-HR-100")
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  "findByBillIds" should "produce a ConnectionIO for a non-empty list" in {
    val cio = repo.findByBillIds(List("118-HR-100", "118-HR-200"))
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  it should "produce a ConnectionIO for an empty list" in {
    val cio = repo.findByBillIds(List.empty)
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  it should "produce a ConnectionIO for a single-element list" in {
    val cio = repo.findByBillIds(List("118-HR-100"))
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  "findBillsNeedingTextCheck" should "produce a ConnectionIO for the query" in {
    val cio = repo.findBillsNeedingTextCheck()
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  "updateTextFields" should "produce a ConnectionIO for the update" in {
    val cio = repo.updateTextFields("118-HR-100", "http://example.com", "XML", "IH", "2024-01-01T00:00:00Z", 1L)
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  "parseNaturalKey" should "split a natural key into congress, type, and number" in {
    val (congress, billType, number) = repo.parseNaturalKey("118-HR-100")
    val _                            = congress shouldBe 118
    val _                            = billType shouldBe "hr"
    number shouldBe "100"
  }

  it should "handle senate bill types" in {
    val (congress, billType, number) = repo.parseNaturalKey("117-S-42")
    val _                            = congress shouldBe 117
    val _                            = billType shouldBe "s"
    number shouldBe "42"
  }

  it should "handle joint resolutions" in {
    val (congress, billType, number) = repo.parseNaturalKey("118-SJRES-5")
    val _                            = congress shouldBe 118
    val _                            = billType shouldBe "sjres"
    number shouldBe "5"
  }

}
