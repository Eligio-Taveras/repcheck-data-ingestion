package com.repcheck.bills.common.persistence

import java.time.Instant

import cats.effect.IO
import cats.effect.unsafe.implicits.global

import doobie.Transactor

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import repcheck.shared.models.congress.dos.bill.BillSubjectDO

import com.repcheck.bills.common.errors.BillSubjectReplaceFailed

class DoobieBillSubjectRepositoryUnitSpec extends AnyFlatSpec with Matchers {

  private val failXa: Transactor[IO] = Transactor.fromDriverManager[IO](
    driver = "org.postgresql.Driver",
    url = "jdbc:postgresql://127.0.0.1:1/test?connectTimeout=1",
    user = "test",
    password = "test",
    logHandler = None,
  )

  private val repo = new DoobieBillSubjectRepository[IO](failXa)

  "replaceAll" should "wrap connection errors in BillSubjectReplaceFailed" in {
    val subject = BillSubjectDO(
      billId = 1L,
      subjectName = "Health",
      embedding = None,
      updateDate = Some(Instant.parse("2024-01-15T00:00:00Z")),
    )

    val ex = intercept[BillSubjectReplaceFailed] {
      repo.replaceAll(1L, List(subject)).unsafeRunSync()
    }
    ex.billId shouldBe 1L
  }

  "findByBillId" should "propagate connection errors" in {
    assertThrows[Exception] {
      repo.findByBillId(1L).unsafeRunSync()
    }
  }

}
