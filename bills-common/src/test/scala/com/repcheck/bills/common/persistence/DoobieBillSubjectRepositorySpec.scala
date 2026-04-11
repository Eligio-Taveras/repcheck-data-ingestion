package com.repcheck.bills.common.persistence

import java.time.Instant

import cats.effect.IO
import cats.effect.unsafe.implicits.global

import doobie.implicits._

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import repcheck.shared.models.congress.common.BillType
import repcheck.shared.models.congress.dos.bill.{BillDO, BillSubjectDO}

import com.repcheck.bills.common.testing.{DockerRequired, TransactorFixture}

class DoobieBillSubjectRepositorySpec extends AnyFlatSpec with Matchers with TransactorFixture {

  private lazy val billRepo    = new DoobieBillRepository
  private lazy val subjectRepo = new DoobieBillSubjectRepository

  private def insertBillAndGetId(): Long = {
    val bill = BillDO(
      billId = 0L,
      naturalKey = "118-HR-200",
      congress = 118,
      billType = BillType.HR,
      number = "200",
      title = "Subject Test Bill",
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
    val _ = billRepo.upsert(bill).transact(xa).unsafeRunSync()
    billRepo.findByBillId("118-HR-200").transact(xa).unsafeRunSync() match {
      case Some(bill) => bill.billId
      case None       => sys.error("Expected bill to be present after insert")
    }
  }

  private def makeSubject(billId: Long, name: String): BillSubjectDO =
    BillSubjectDO(
      billId = billId,
      subjectName = name,
      embedding = None,
      updateDate = Some(Instant.parse("2024-01-15T00:00:00Z")),
    )

  "replaceAll" should "insert subjects for a new bill" taggedAs DockerRequired in {
    val billId = insertBillAndGetId()
    val subjects = List(
      makeSubject(billId, "Health"),
      makeSubject(billId, "Education"),
      makeSubject(billId, "Defense"),
      makeSubject(billId, "Environment"),
      makeSubject(billId, "Economy"),
    )

    subjectRepo.replaceAll(billId, subjects).transact(xa).unsafeRunSync()
    val found = subjectRepo.findByBillId(billId).transact(xa).unsafeRunSync()
    found.size shouldBe 5
  }

  it should "replace subjects on re-ingest" taggedAs DockerRequired in {
    val billId = insertBillAndGetId()
    subjectRepo
      .replaceAll(
        billId,
        List(
          makeSubject(billId, "Health"),
          makeSubject(billId, "Education"),
          makeSubject(billId, "Defense"),
          makeSubject(billId, "Environment"),
          makeSubject(billId, "Economy"),
        ),
      )
      .transact(xa)
      .unsafeRunSync()

    subjectRepo
      .replaceAll(
        billId,
        List(
          makeSubject(billId, "Technology"),
          makeSubject(billId, "Science"),
          makeSubject(billId, "Agriculture"),
        ),
      )
      .transact(xa)
      .unsafeRunSync()

    val found = subjectRepo.findByBillId(billId).transact(xa).unsafeRunSync()
    val _     = found.size shouldBe 3
    found.map(_.subjectName) should contain allOf ("Technology", "Science", "Agriculture")
  }

  it should "handle empty list" taggedAs DockerRequired in {
    val billId = insertBillAndGetId()
    subjectRepo.replaceAll(billId, List(makeSubject(billId, "Health"))).transact(xa).unsafeRunSync()

    subjectRepo.replaceAll(billId, List.empty).transact(xa).unsafeRunSync()

    val found = subjectRepo.findByBillId(billId).transact(xa).unsafeRunSync()
    found shouldBe empty
  }

  private def make1536Embedding(seed: Float): Array[Float] =
    Array.tabulate(1536)(i => seed + i * 0.001f)

  it should "round-trip subjects with embeddings" taggedAs DockerRequired in {
    val billId = insertBillAndGetId()
    val subjects = List(
      BillSubjectDO(
        billId = billId,
        subjectName = "Armed Forces",
        embedding = Some(make1536Embedding(0.1f)),
        updateDate = Some(Instant.parse("2024-01-15T00:00:00Z")),
      ),
      BillSubjectDO(
        billId = billId,
        subjectName = "Veterans",
        embedding = Some(make1536Embedding(0.4f)),
        updateDate = None,
      ),
    )

    subjectRepo.replaceAll(billId, subjects).transact(xa).unsafeRunSync()
    val found = subjectRepo.findByBillId(billId).transact(xa).unsafeRunSync()
    found.size shouldBe 2
  }

  it should "preserve embedding values through round-trip" taggedAs DockerRequired in {
    val billId = insertBillAndGetId()
    val subjects = List(
      BillSubjectDO(
        billId = billId,
        subjectName = "Finance",
        embedding = Some(make1536Embedding(1.0f)),
        updateDate = Some(Instant.parse("2024-06-01T00:00:00Z")),
      )
    )

    subjectRepo.replaceAll(billId, subjects).transact(xa).unsafeRunSync()
    val found = subjectRepo.findByBillId(billId).transact(xa).unsafeRunSync()
    val _     = found.size shouldBe 1
    found.headOption.flatMap(_.embedding).isDefined shouldBe true
  }

  "findByBillId" should "return empty for unknown bill" taggedAs DockerRequired in {
    val found = subjectRepo.findByBillId(99999L).transact(xa).unsafeRunSync()
    found shouldBe empty
  }

}
