package com.repcheck.bills.text.integration

import cats.effect.IO
import cats.effect.unsafe.implicits.global

import doobie.implicits._

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import repcheck.shared.models.congress.common.{BillType, Chamber}
import repcheck.shared.models.congress.dos.bill.BillDO

import com.repcheck.bills.common.persistence.{DoobieBillRepository, DoobieBillTextVersionRepository}
import com.repcheck.bills.common.testing.{DockerRequired, PubSubEmulatorFixture, TransactorFixture}

/**
 * E2E-style tests that validate infrastructure connectivity and data flow using real AlloyDB Omni (Docker) and Pub/Sub
 * emulator (Docker). Both containers are started automatically by the test fixtures.
 *
 * These tests verify: 1. Bills can be inserted and queried in the database 2. Bill text versions can be stored and
 * retrieved 3. Pub/Sub emulator accepts and returns messages
 */
class DockerE2ESpec extends AnyFlatSpec with Matchers with TransactorFixture with PubSubEmulatorFixture {

  private val billRepo        = new DoobieBillRepository()
  private val textVersionRepo = new DoobieBillTextVersionRepository()

  private def seedBill(naturalKey: String, number: String): Long = {
    val bill = BillDO(
      billId = 0L,
      naturalKey = naturalKey,
      congress = 118,
      billType = BillType.HR,
      number = number,
      title = "E2E Test Bill",
      originChamber = Some(Chamber.House),
      originChamberCode = Some("H"),
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
    billRepo.upsert(bill).transact(xa).unsafeRunSync()
  }

  "Docker E2E" should "insert and query bill records in database" taggedAs DockerRequired in {
    val _ = seedBill("118-HR-900", "900")
    val _ = seedBill("118-HR-901", "901")

    val billCount = sql"SELECT COUNT(*) FROM bills"
      .query[Long]
      .unique
      .transact(xa)
      .unsafeRunSync()

    billCount should be >= 2L
  }

  it should "store and retrieve bill text versions" taggedAs DockerRequired in {
    val dbBillId = seedBill("118-HR-902", "902")

    // Insert a text version directly
    val _ = sql"""
      INSERT INTO bill_text_versions (bill_id, version_code, version_type, content)
      VALUES ($dbBillId, 'IH', 'Introduced in House', 'Test bill text content for E2E')
    """.update.run.transact(xa).unsafeRunSync()

    val versions = textVersionRepo.findByBillId(dbBillId).transact(xa).unsafeRunSync()
    val _        = versions.size shouldBe 1
    versions.headOption.flatMap(_.content).getOrElse("") should include("E2E")
  }

  it should "publish and pull messages on Pub/Sub emulator" taggedAs DockerRequired in {
    val messageId = publishMessage(
      """{"test":"e2e-verification"}""",
      Map("eventType" -> "E2ETest"),
    )
    val _ = messageId should not be empty

    // Brief delay for message propagation in emulator
    Thread.sleep(500L)

    val messages = pullMessages()
    val _        = messages should not be empty
    messages.headOption.map(_.getData.toStringUtf8).getOrElse("") should include("e2e-verification")
  }

}
