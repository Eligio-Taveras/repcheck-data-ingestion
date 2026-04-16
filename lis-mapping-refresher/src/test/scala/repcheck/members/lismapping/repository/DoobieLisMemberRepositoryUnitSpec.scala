package repcheck.members.lismapping.repository

import java.time.Instant

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import repcheck.shared.models.congress.dos.member.LisMemberDO

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

  "findByNaturalKey" should "produce a ConnectionIO for a non-empty lisId" in {
    val cio = repo.findByNaturalKey("S428")
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  it should "produce a ConnectionIO for an empty-string lisId" in {
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

  "DoobieLisMemberRepository" should "implement LisMemberRepository trait" in {
    repo shouldBe a[LisMemberRepository]
  }

}
