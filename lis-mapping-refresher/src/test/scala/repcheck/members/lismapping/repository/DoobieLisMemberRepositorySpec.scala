package repcheck.members.lismapping.repository

import java.time.Instant

import cats.effect.unsafe.implicits.global

import doobie.implicits._

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import repcheck.members.common.testing.DockerRequired
import repcheck.members.lismapping.testing.TransactorFixture
import repcheck.shared.models.congress.dos.member.LisMemberDO

class DoobieLisMemberRepositorySpec extends AnyFlatSpec with Matchers with TransactorFixture {

  private lazy val repo = new DoobieLisMemberRepository

  private def makeMember(naturalKey: String, firstName: String = "Jane"): LisMemberDO = LisMemberDO(
    id = 0L,
    naturalKey = naturalKey,
    firstName = Some(firstName),
    lastName = Some("Doe"),
    party = Some("D"),
    state = Some("NY"),
    lastVerified = Some(Instant.parse("2024-06-15T00:00:00Z")),
    createdAt = None,
  )

  "upsertByNaturalKey" should "insert a new LIS member and return the generated id" taggedAs DockerRequired in {
    val member = makeMember("S428")

    val id = repo.upsertByNaturalKey(member).transact(xa).unsafeRunSync()

    id should be > 0L
  }

  it should "return the same id when upserting an existing natural_key" taggedAs DockerRequired in {
    val member = makeMember("S429")

    val firstId  = repo.upsertByNaturalKey(member).transact(xa).unsafeRunSync()
    val secondId = repo.upsertByNaturalKey(member.copy(firstName = Some("Janet"))).transact(xa).unsafeRunSync()

    secondId shouldBe firstId
  }

  it should "update mutable fields on conflict" taggedAs DockerRequired in {
    val initial = makeMember("S430", firstName = "Initial")
    val updated = initial.copy(
      firstName = Some("Updated"),
      lastName = Some("Changed"),
      party = Some("R"),
      state = Some("CA"),
      lastVerified = Some(Instant.parse("2025-01-10T00:00:00Z")),
    )

    val _     = repo.upsertByNaturalKey(initial).transact(xa).unsafeRunSync()
    val _     = repo.upsertByNaturalKey(updated).transact(xa).unsafeRunSync()
    val found = repo.findByNaturalKey("S430").transact(xa).unsafeRunSync()

    found match {
      case Some(row) =>
        val _ = row.firstName shouldBe Some("Updated")
        val _ = row.lastName shouldBe Some("Changed")
        val _ = row.party shouldBe Some("R")
        val _ = row.state shouldBe Some("CA")
        row.lastVerified shouldBe Some(Instant.parse("2025-01-10T00:00:00Z"))
      case None => sys.error("Expected LIS member row after upsert")
    }
  }

  it should "insert a minimal LIS member with all optional fields None" taggedAs DockerRequired in {
    val minimal = LisMemberDO(
      id = 0L,
      naturalKey = "S431",
      firstName = None,
      lastName = None,
      party = None,
      state = None,
      lastVerified = None,
      createdAt = None,
    )

    val id    = repo.upsertByNaturalKey(minimal).transact(xa).unsafeRunSync()
    val found = repo.findByNaturalKey("S431").transact(xa).unsafeRunSync()

    val _ = id should be > 0L
    found match {
      case Some(row) =>
        val _ = row.firstName shouldBe None
        val _ = row.lastName shouldBe None
        val _ = row.party shouldBe None
        val _ = row.state shouldBe None
        row.lastVerified shouldBe None
      case None => sys.error("Expected LIS member row after upsert")
    }
  }

  "findByNaturalKey" should "return None for an unknown natural_key" taggedAs DockerRequired in {
    val found = repo.findByNaturalKey("S-UNKNOWN").transact(xa).unsafeRunSync()
    found shouldBe None
  }

  it should "roundtrip an inserted LIS member" taggedAs DockerRequired in {
    val member = makeMember("S432")
    val id     = repo.upsertByNaturalKey(member).transact(xa).unsafeRunSync()
    val found  = repo.findByNaturalKey("S432").transact(xa).unsafeRunSync()

    found match {
      case Some(row) =>
        val _ = row.id shouldBe id
        val _ = row.naturalKey shouldBe "S432"
        row.firstName shouldBe Some("Jane")
      case None => sys.error("Expected LIS member row after upsert")
    }
  }

  "findById" should "return None for an unknown id" taggedAs DockerRequired in {
    val found = repo.findById(-99L).transact(xa).unsafeRunSync()
    found shouldBe None
  }

  it should "roundtrip an inserted LIS member" taggedAs DockerRequired in {
    val member = makeMember("S433")
    val id     = repo.upsertByNaturalKey(member).transact(xa).unsafeRunSync()
    val found  = repo.findById(id).transact(xa).unsafeRunSync()

    found match {
      case Some(row) =>
        val _ = row.id shouldBe id
        row.naturalKey shouldBe "S433"
      case None => sys.error("Expected LIS member row after upsert")
    }
  }

}
