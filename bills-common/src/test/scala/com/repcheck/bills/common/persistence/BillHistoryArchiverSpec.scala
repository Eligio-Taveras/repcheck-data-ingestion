package com.repcheck.bills.common.persistence

import cats.effect.IO
import cats.effect.unsafe.implicits.global

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class BillHistoryArchiverSpec extends AnyFlatSpec with Matchers {

  private val stubArchiver: BillHistoryArchiver[IO] = new BillHistoryArchiver[IO] {
    override def archiveBill(billId: String): IO[Long] = IO.pure(1L)
  }

  "BillHistoryArchiver" should "compile with a stub implementation" in {
    stubArchiver.toString should not be empty
  }

  it should "return a history ID from archiveBill for a stub" in {
    stubArchiver.archiveBill("118-HR-1").unsafeRunSync() shouldBe 1L
  }

}
