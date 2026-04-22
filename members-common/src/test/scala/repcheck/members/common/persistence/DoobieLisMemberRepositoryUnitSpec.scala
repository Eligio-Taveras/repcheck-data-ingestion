package repcheck.members.common.persistence

import java.time.Instant

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import repcheck.shared.models.congress.dos.member.LisMemberDO

/**
 * Pure unit coverage for [[DoobieLisMemberRepository]]. Confirms every method produces a well-formed `ConnectionIO`
 * without depending on a database — shape-level smoke tests that the Doobie interpolator compiles and executes its
 * fragment assembly. Real-SQL coverage lives in [[DoobieLisMemberRepositorySpec]] (DockerRequired).
 */
class DoobieLisMemberRepositoryUnitSpec extends AnyFlatSpec with Matchers {

  private val repo = new DoobieLisMemberRepository

  private val populatedMember = LisMemberDO(
    id = 0L,
    naturalKey = "S428",
    firstName = Some("Jane"),
    lastName = Some("Doe"),
    party = Some("D"),
    state = Some("NY"),
    lastVerified = Some(Instant.parse("2024-06-15T00:00:00Z")),
    createdAt = Some(Instant.parse("2024-06-01T00:00:00Z")),
  )

  private val minimalMember = LisMemberDO(
    id = 0L,
    naturalKey = "S999",
    firstName = None,
    lastName = None,
    party = None,
    state = None,
    lastVerified = None,
    createdAt = None,
  )

  "upsertByNaturalKey" should "produce a ConnectionIO for a fully populated LIS member" in {
    val cio = repo.upsertByNaturalKey(populatedMember)
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  it should "produce a ConnectionIO for a minimal LIS member with all None optional fields" in {
    val cio = repo.upsertByNaturalKey(minimalMember)
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  it should "produce a ConnectionIO when only firstName is populated" in {
    val cio = repo.upsertByNaturalKey(minimalMember.copy(firstName = Some("Solo")))
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  it should "produce a ConnectionIO when only lastVerified is populated" in {
    val cio =
      repo.upsertByNaturalKey(minimalMember.copy(lastVerified = Some(Instant.parse("2025-01-01T00:00:00Z"))))
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  "findByNaturalKey" should "produce a ConnectionIO for a non-empty natural key" in {
    val cio = repo.findByNaturalKey("S428")
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  it should "produce a ConnectionIO for an empty-string natural key" in {
    val cio = repo.findByNaturalKey("")
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  "findById" should "produce a ConnectionIO for a positive id" in {
    val cio = repo.findById(42L)
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  it should "produce a ConnectionIO for a zero id" in {
    val cio = repo.findById(0L)
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  it should "produce a ConnectionIO for a negative id" in {
    val cio = repo.findById(-1L)
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  "findByNaturalKeys" should "short-circuit to the empty map for an empty input list without touching the DB" in {
    import cats.effect.IO
    import cats.effect.unsafe.implicits.global

    import doobie.Transactor

    // An in-memory H2 transactor is sufficient because the empty-input branch must never issue SQL.
    val xa = Transactor.fromDriverManager[IO](
      driver = "org.h2.Driver",
      url = "jdbc:h2:mem:lismemberempty;DB_CLOSE_DELAY=-1",
      user = "",
      password = "",
      logHandler = None,
    )
    val result = {
      import doobie.implicits._
      repo.findByNaturalKeys(List.empty).transact(xa).unsafeRunSync()
    }
    result shouldBe Map.empty[String, Long]
  }

  it should "produce a ConnectionIO for a single-element list" in {
    val cio = repo.findByNaturalKeys(List("S428"))
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  it should "produce a ConnectionIO for a multi-element list" in {
    val cio = repo.findByNaturalKeys(List("S1", "S2", "S3"))
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  "DoobieLisMemberRepository" should "implement LisMemberRepository trait" in {
    repo shouldBe a[LisMemberRepository]
  }

}
