package com.repcheck.bills.common.persistence

import cats.effect.IO
import cats.effect.unsafe.implicits.global

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import repcheck.shared.models.congress.dos.bill.BillSubjectDO

class BillSubjectRepositorySpec extends AnyFlatSpec with Matchers {

  private val stubRepo: BillSubjectRepository[IO] = new BillSubjectRepository[IO] {

    override def replaceAll(billId: Long, subjects: List[BillSubjectDO]): IO[Unit] = IO.unit

    override def findByBillId(billId: Long): IO[List[BillSubjectDO]] = IO.pure(List.empty)
  }

  "BillSubjectRepository" should "compile with a stub implementation" in {
    stubRepo.toString should not be empty
  }

  it should "return Unit from replaceAll for a stub" in {
    stubRepo.replaceAll(1L, List.empty).unsafeRunSync() shouldBe (())
  }

  it should "return empty list from findByBillId for a stub" in {
    stubRepo.findByBillId(1L).unsafeRunSync() shouldBe List.empty
  }

}
