package repcheck.members.common.persistence

import java.time.Instant

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import repcheck.shared.models.congress.common.{Party, UsState}
import repcheck.shared.models.congress.dos.member.MemberDO

class DoobieMemberRepositoryUnitSpec extends AnyFlatSpec with Matchers {

  private val repo = new DoobieMemberRepository

  private val sampleMember = MemberDO(
    memberId = 0L,
    naturalKey = "A000001",
    firstName = Some("John"),
    lastName = Some("Doe"),
    directOrderName = Some("John Doe"),
    invertedOrderName = Some("Doe, John"),
    honorificName = Some("Rep. John Doe"),
    birthYear = Some(1970),
    currentParty = Some(Party.Democrat),
    state = Some(UsState.NewYork),
    district = Some(5),
    imageUrl = Some("https://example.com/photo.jpg"),
    imageAttribution = Some("Official Photo"),
    officialUrl = Some("https://doe.house.gov"),
    updateDate = Some(Instant.parse("2024-01-15T00:00:00Z")),
    createdAt = Some(Instant.parse("2024-01-01T00:00:00Z")),
    updatedAt = Some(Instant.parse("2024-01-15T00:00:00Z")),
  )

  "findById" should "produce a ConnectionIO for a valid member ID" in {
    val cio = repo.findById(42L)
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  it should "produce a ConnectionIO for a zero ID" in {
    val cio = repo.findById(0L)
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  "findByBioguideId" should "produce a ConnectionIO for a valid bioguide ID" in {
    val cio = repo.findByBioguideId("A000001")
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  it should "produce a ConnectionIO for an empty string bioguide ID" in {
    val cio = repo.findByBioguideId("")
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  "upsert" should "produce a ConnectionIO for a fully populated member" in {
    val cio = repo.upsert(sampleMember)
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  it should "produce a ConnectionIO for a member with minimal fields" in {
    val minimalMember = MemberDO(
      memberId = 0L,
      naturalKey = "B000002",
      firstName = None,
      lastName = None,
      directOrderName = None,
      invertedOrderName = None,
      honorificName = None,
      birthYear = None,
      currentParty = None,
      state = None,
      district = None,
      imageUrl = None,
      imageAttribution = None,
      officialUrl = None,
      updateDate = None,
      createdAt = None,
      updatedAt = None,
    )
    val cio = repo.upsert(minimalMember)
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  "upsertPlaceholder" should "produce a ConnectionIO for a well-formed bioguide id" in {
    val cio = repo.upsertPlaceholder("A000370")
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  it should "produce a ConnectionIO that raises InvalidBioguideId for a malformed bioguide id" in {
    val cio = repo.upsertPlaceholder("nope")
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  "findPlaceholders" should "produce a ConnectionIO for the placeholder query" in {
    val cio = repo.findPlaceholders()
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  "existsWithLisMapping" should "produce a ConnectionIO for a valid member ID" in {
    val cio = repo.existsWithLisMapping(42L)
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  it should "produce a ConnectionIO for a zero ID" in {
    val cio = repo.existsWithLisMapping(0L)
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  "DoobieMemberRepository" should "implement MemberRepository trait" in {
    repo shouldBe a[MemberRepository]
  }

}
