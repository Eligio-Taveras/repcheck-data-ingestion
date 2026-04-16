package repcheck.members.common.persistence

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import repcheck.shared.models.congress.dos.member.MemberPartyHistoryDO

class DoobieMemberPartyHistoryRepositoryUnitSpec extends AnyFlatSpec with Matchers {

  private val repo = new DoobieMemberPartyHistoryRepository

  private val sampleEntry = MemberPartyHistoryDO(
    id = 0L,
    memberId = 1L,
    partyName = Some("Democrat"),
    partyAbbreviation = Some("D"),
    startYear = Some(2005),
  )

  "appendNew" should "produce a ConnectionIO for a single entry" in {
    val cio = repo.appendNew(1L, List(sampleEntry))
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  it should "handle an empty entries list" in {
    val cio = repo.appendNew(1L, List.empty)
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  it should "handle multiple entries" in {
    val second = sampleEntry.copy(
      partyName = Some("Independent"),
      partyAbbreviation = Some("I"),
      startYear = Some(2011),
    )
    val cio = repo.appendNew(1L, List(sampleEntry, second))
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  it should "handle entries with all optional fields set to None" in {
    val minimal = MemberPartyHistoryDO(
      id = 0L,
      memberId = 1L,
      partyName = None,
      partyAbbreviation = None,
      startYear = None,
    )
    val cio = repo.appendNew(1L, List(minimal))
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  "findByMemberId" should "produce a ConnectionIO for a valid member ID" in {
    val cio = repo.findByMemberId(1L)
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  it should "produce a ConnectionIO for a zero ID" in {
    val cio = repo.findByMemberId(0L)
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  "DoobieMemberPartyHistoryRepository" should "implement MemberPartyHistoryRepository trait" in {
    repo shouldBe a[MemberPartyHistoryRepository]
  }

}
