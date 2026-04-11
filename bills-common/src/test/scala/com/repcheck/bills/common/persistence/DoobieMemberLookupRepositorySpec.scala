package com.repcheck.bills.common.persistence

import cats.effect.IO
import cats.effect.unsafe.implicits.global

import doobie.implicits._

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.repcheck.bills.common.testing.{DockerRequired, TransactorFixture}

class DoobieMemberLookupRepositorySpec extends AnyFlatSpec with Matchers with TransactorFixture {

  private lazy val repo = new DoobieMemberLookupRepository

  "findIdByNaturalKey" should "return id for a known member" taggedAs DockerRequired in {
    val result = repo.findIdByNaturalKey("TEST-M001").transact(xa).unsafeRunSync()
    val _      = result shouldBe defined
    result.foreach(id => id should be > 0L)
  }

  it should "return None for an unknown member" taggedAs DockerRequired in {
    val result = repo.findIdByNaturalKey("NONEXISTENT-9999").transact(xa).unsafeRunSync()
    result shouldBe None
  }

  it should "return consistent id across multiple calls" taggedAs DockerRequired in {
    val first  = repo.findIdByNaturalKey("TEST-M002").transact(xa).unsafeRunSync()
    val second = repo.findIdByNaturalKey("TEST-M002").transact(xa).unsafeRunSync()
    first shouldBe second
  }

}
