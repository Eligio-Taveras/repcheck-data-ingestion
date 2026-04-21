package repcheck.members.common.persistence

import java.time.Instant

import cats.effect.IO
import cats.effect.unsafe.implicits.global

import doobie.Transactor
import doobie.implicits._

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import repcheck.shared.models.congress.dos.member.MemberLisMappingDO

class DoobieLisMappingRepositoryUnitSpec extends AnyFlatSpec with Matchers {

  private val repo = new DoobieLisMappingRepository

  private val sampleMapping = MemberLisMappingDO(
    id = 0L,
    memberId = 42L,
    lisMemberId = 7L,
    lastVerified = Instant.parse("2024-06-15T00:00:00Z"),
  )

  "upsert" should "produce a ConnectionIO for a single mapping" in {
    val cio = repo.upsert(sampleMapping)
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  it should "produce a ConnectionIO for a mapping with different ids" in {
    val cio = repo.upsert(sampleMapping.copy(memberId = 99L, lisMemberId = 100L))
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  "upsertBatch" should "produce a ConnectionIO for an empty batch" in {
    val cio = repo.upsertBatch(List.empty)
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  it should "produce a ConnectionIO for a single-element batch" in {
    val cio = repo.upsertBatch(List(sampleMapping))
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  it should "produce a ConnectionIO for a multi-element batch" in {
    val second = sampleMapping.copy(memberId = 43L, lisMemberId = 8L)
    val third  = sampleMapping.copy(memberId = 44L, lisMemberId = 9L)
    val cio    = repo.upsertBatch(List(sampleMapping, second, third))
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  "findByLisMemberId" should "produce a ConnectionIO for a positive id" in {
    val cio = repo.findByLisMemberId(1L)
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  it should "produce a ConnectionIO for a zero id" in {
    val cio = repo.findByLisMemberId(0L)
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  "findByMemberId" should "produce a ConnectionIO for a positive id" in {
    val cio = repo.findByMemberId(1L)
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  it should "produce a ConnectionIO for a zero id" in {
    val cio = repo.findByMemberId(0L)
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  // The batch read short-circuits to Map.empty without hitting the DB when the input is empty; verify this by
  // running against an h2 transactor that has no `member_lis_mapping` table. If the short-circuit fails, the
  // call would raise a table-not-found error at SQL compile time.
  "findByLisMemberIds" should "short-circuit to an empty map for an empty input without touching the database" in {
    val h2: Transactor[IO] = Transactor.fromDriverManager[IO](
      driver = "org.h2.Driver",
      url = "jdbc:h2:mem:lismapping-batch-empty;DB_CLOSE_DELAY=-1",
      user = "",
      password = "",
      logHandler = None,
    )
    val result = repo.findByLisMemberIds(List.empty).transact(h2).unsafeRunSync()
    result shouldBe Map.empty[Long, Long]
  }

  it should "produce a ConnectionIO for a non-empty input" in {
    val cio = repo.findByLisMemberIds(List(1L, 2L, 3L))
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  it should "produce a ConnectionIO for a single-element input" in {
    val cio = repo.findByLisMemberIds(List(42L))
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  "DoobieLisMappingRepository" should "implement LisMappingRepository trait" in {
    repo shouldBe a[LisMappingRepository]
  }

}
