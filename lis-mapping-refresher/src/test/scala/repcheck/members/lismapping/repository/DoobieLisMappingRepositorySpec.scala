package repcheck.members.lismapping.repository

import java.time.Instant

import cats.effect.unsafe.implicits.global

import doobie.implicits._

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import repcheck.members.common.testing.DockerRequired
import repcheck.members.lismapping.testing.TransactorFixture
import repcheck.shared.models.congress.dos.member.{LisMemberDO, MemberLisMappingDO}

class DoobieLisMappingRepositorySpec extends AnyFlatSpec with Matchers with TransactorFixture {

  private lazy val lisMemberRepo  = new DoobieLisMemberRepository
  private lazy val lisMappingRepo = new DoobieLisMappingRepository

  private def insertLisMember(naturalKey: String): Long = {
    val lisMember = LisMemberDO(
      id = 0L,
      naturalKey = naturalKey,
      firstName = Some("Test"),
      lastName = Some("Senator"),
      party = Some("D"),
      state = Some("NY"),
      lastVerified = Some(Instant.parse("2024-06-15T00:00:00Z")),
      createdAt = None,
    )
    lisMemberRepo.upsertByNaturalKey(lisMember).transact(xa).unsafeRunSync()
  }

  private def makeMapping(memberId: Long, lisMemberId: Long, verified: Instant): MemberLisMappingDO =
    MemberLisMappingDO(
      id = 0L,
      memberId = memberId,
      lisMemberId = lisMemberId,
      lastVerified = verified,
    )

  "upsert" should "return Inserted for a brand-new mapping" taggedAs DockerRequired in {
    val memberId    = insertMember("LIS-M001")
    val lisMemberId = insertLisMember("S500")
    val mapping     = makeMapping(memberId, lisMemberId, Instant.parse("2024-06-15T00:00:00Z"))

    val result = lisMappingRepo.upsert(mapping).transact(xa).unsafeRunSync()

    result shouldBe UpsertResult.Inserted
  }

  it should "return Updated when the (member_id, lis_member_id) pair already exists" taggedAs DockerRequired in {
    val memberId    = insertMember("LIS-M002")
    val lisMemberId = insertLisMember("S501")
    val initial     = makeMapping(memberId, lisMemberId, Instant.parse("2024-06-15T00:00:00Z"))
    val refreshed   = initial.copy(lastVerified = Instant.parse("2025-01-01T00:00:00Z"))

    val firstResult  = lisMappingRepo.upsert(initial).transact(xa).unsafeRunSync()
    val secondResult = lisMappingRepo.upsert(refreshed).transact(xa).unsafeRunSync()

    val _ = firstResult shouldBe UpsertResult.Inserted
    secondResult shouldBe UpsertResult.Updated
  }

  it should "persist last_verified on update" taggedAs DockerRequired in {
    val memberId    = insertMember("LIS-M003")
    val lisMemberId = insertLisMember("S502")
    val initial     = makeMapping(memberId, lisMemberId, Instant.parse("2024-06-15T00:00:00Z"))
    val refreshed   = initial.copy(lastVerified = Instant.parse("2025-02-02T00:00:00Z"))

    val _     = lisMappingRepo.upsert(initial).transact(xa).unsafeRunSync()
    val _     = lisMappingRepo.upsert(refreshed).transact(xa).unsafeRunSync()
    val found = lisMappingRepo.findByMemberId(memberId).transact(xa).unsafeRunSync()

    found match {
      case Some(row) => row.lastVerified shouldBe Instant.parse("2025-02-02T00:00:00Z")
      case None      => sys.error("Expected mapping row")
    }
  }

  "upsertBatch" should "return an empty list for an empty batch" taggedAs DockerRequired in {
    val results = lisMappingRepo.upsertBatch(List.empty).transact(xa).unsafeRunSync()
    results shouldBe empty
  }

  it should "return a list of 3 results in the same order as the input batch" taggedAs DockerRequired in {
    val m1  = insertMember("LIS-M001")
    val m2  = insertMember("LIS-M002")
    val m3  = insertMember("LIS-M003")
    val l1  = insertLisMember("S600")
    val l2  = insertLisMember("S601")
    val l3  = insertLisMember("S602")
    val now = Instant.parse("2024-06-15T00:00:00Z")
    val batch = List(
      makeMapping(m1, l1, now),
      makeMapping(m2, l2, now),
      makeMapping(m3, l3, now),
    )

    val results = lisMappingRepo.upsertBatch(batch).transact(xa).unsafeRunSync()

    val _ = results.size shouldBe 3
    results shouldBe List(UpsertResult.Inserted, UpsertResult.Inserted, UpsertResult.Inserted)
  }

  it should "mix Inserted and Updated results when some pairs already exist" taggedAs DockerRequired in {
    val m1    = insertMember("LIS-M001")
    val m2    = insertMember("LIS-M002")
    val l1    = insertLisMember("S700")
    val l2    = insertLisMember("S701")
    val now   = Instant.parse("2024-06-15T00:00:00Z")
    val later = Instant.parse("2025-01-01T00:00:00Z")
    val _     = lisMappingRepo.upsert(makeMapping(m1, l1, now)).transact(xa).unsafeRunSync()

    val results = lisMappingRepo
      .upsertBatch(List(makeMapping(m1, l1, later), makeMapping(m2, l2, now)))
      .transact(xa)
      .unsafeRunSync()

    results shouldBe List(UpsertResult.Updated, UpsertResult.Inserted)
  }

  "findByLisMemberId" should "return None for an unknown lis_member_id" taggedAs DockerRequired in {
    val found = lisMappingRepo.findByLisMemberId(-1L).transact(xa).unsafeRunSync()
    found shouldBe None
  }

  it should "roundtrip an inserted mapping" taggedAs DockerRequired in {
    val memberId    = insertMember("LIS-M004")
    val lisMemberId = insertLisMember("S800")
    val mapping     = makeMapping(memberId, lisMemberId, Instant.parse("2024-06-15T00:00:00Z"))
    val _           = lisMappingRepo.upsert(mapping).transact(xa).unsafeRunSync()

    val found = lisMappingRepo.findByLisMemberId(lisMemberId).transact(xa).unsafeRunSync()

    found match {
      case Some(row) =>
        val _ = row.memberId shouldBe memberId
        row.lisMemberId shouldBe lisMemberId
      case None => sys.error("Expected mapping row")
    }
  }

  "findByMemberId" should "return None for an unknown member_id" taggedAs DockerRequired in {
    val found = lisMappingRepo.findByMemberId(-1L).transact(xa).unsafeRunSync()
    found shouldBe None
  }

  it should "roundtrip an inserted mapping" taggedAs DockerRequired in {
    val memberId    = insertMember("LIS-M005")
    val lisMemberId = insertLisMember("S900")
    val mapping     = makeMapping(memberId, lisMemberId, Instant.parse("2024-06-15T00:00:00Z"))
    val _           = lisMappingRepo.upsert(mapping).transact(xa).unsafeRunSync()

    val found = lisMappingRepo.findByMemberId(memberId).transact(xa).unsafeRunSync()

    found match {
      case Some(row) =>
        val _ = row.memberId shouldBe memberId
        row.lisMemberId shouldBe lisMemberId
      case None => sys.error("Expected mapping row")
    }
  }

}
