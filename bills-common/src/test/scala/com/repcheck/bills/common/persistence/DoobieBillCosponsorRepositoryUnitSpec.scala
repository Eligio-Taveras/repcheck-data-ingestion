package com.repcheck.bills.common.persistence

import java.time.LocalDate

import cats.effect.IO
import cats.effect.unsafe.implicits.global

import doobie.Transactor

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import repcheck.shared.models.congress.dos.bill.BillCosponsorDO

import com.repcheck.bills.common.errors.BillCosponsorReplaceFailed

class DoobieBillCosponsorRepositoryUnitSpec extends AnyFlatSpec with Matchers {

  private val failXa: Transactor[IO] = Transactor.fromDriverManager[IO](
    driver = "org.postgresql.Driver",
    url = "jdbc:postgresql://127.0.0.1:1/test?connectTimeout=1",
    user = "test",
    password = "test",
    logHandler = None,
  )

  private val repo = new DoobieBillCosponsorRepository[IO](failXa)

  "replaceAll" should "wrap connection errors in BillCosponsorReplaceFailed" in {
    val cosponsor = BillCosponsorDO(
      billId = 1L,
      memberId = 2L,
      isOriginalCosponsor = Some(true),
      sponsorshipDate = Some(LocalDate.parse("2024-01-15")),
    )

    val ex = intercept[BillCosponsorReplaceFailed] {
      repo.replaceAll(1L, List(cosponsor)).unsafeRunSync()
    }
    ex.billId shouldBe 1L
  }

  "findByBillId" should "propagate connection errors" in {
    assertThrows[Exception] {
      repo.findByBillId(1L).unsafeRunSync()
    }
  }

}
