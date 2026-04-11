package com.repcheck.bills.common.persistence

import cats.effect.IO
import cats.effect.unsafe.implicits.global

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class MemberLookupRepositorySpec extends AnyFlatSpec with Matchers {

  private val stubRepo: MemberLookupRepository[IO] = new MemberLookupRepository[IO] {
    override def findIdByNaturalKey(naturalKey: String): IO[Option[Long]] = IO.pure(None)
  }

  "MemberLookupRepository" should "compile with a stub implementation" in {
    stubRepo.toString should not be empty
  }

  it should "return None from findIdByNaturalKey for a stub" in {
    stubRepo.findIdByNaturalKey("A000001").unsafeRunSync() shouldBe None
  }

}
