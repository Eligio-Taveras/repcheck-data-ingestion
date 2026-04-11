package com.repcheck.bills.common.persistence

import cats.effect.IO
import cats.effect.unsafe.implicits.global

import doobie.Transactor

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import repcheck.shared.models.congress.common.FormatType
import repcheck.shared.models.congress.dos.bill.BillTextVersionDO

import com.repcheck.bills.common.errors.BillTextVersionInsertFailed

class DoobieBillTextVersionRepositoryUnitSpec extends AnyFlatSpec with Matchers {

  private val failXa: Transactor[IO] = Transactor.fromDriverManager[IO](
    driver = "org.postgresql.Driver",
    url = "jdbc:postgresql://127.0.0.1:1/test?connectTimeout=1",
    user = "test",
    password = "test",
    logHandler = None,
  )

  private val repo = new DoobieBillTextVersionRepository[IO](failXa)

  private val sampleVersion = BillTextVersionDO(
    id = 0L,
    billId = 1L,
    versionCode = "IH",
    versionType = "IH",
    versionDate = None,
    formatType = Some(FormatType.FormattedXml),
    url = Some("http://example.com/text"),
    content = None,
    embedding = None,
    fetchedAt = None,
    createdAt = None,
  )

  "insertVersion" should "wrap connection errors in BillTextVersionInsertFailed" in {
    val ex = intercept[BillTextVersionInsertFailed] {
      repo.insertVersion(sampleVersion).unsafeRunSync()
    }
    ex.billId shouldBe 1L
  }

  "findByBillId" should "propagate connection errors" in {
    assertThrows[Exception] {
      repo.findByBillId(1L).unsafeRunSync()
    }
  }

  "findLatestByBillId" should "propagate connection errors" in {
    assertThrows[Exception] {
      repo.findLatestByBillId(1L).unsafeRunSync()
    }
  }

  "storeAndUpdateBill" should "wrap connection errors in BillTextVersionInsertFailed" in {
    val ex = intercept[BillTextVersionInsertFailed] {
      repo.storeAndUpdateBill(sampleVersion).unsafeRunSync()
    }
    ex.billId shouldBe 1L
  }

  "buildBillTextFieldsUpdate" should "construct a ConnectionIO for the bill text fields update" in {
    val cio = repo.buildBillTextFieldsUpdate(sampleVersion, 42L)
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

}
