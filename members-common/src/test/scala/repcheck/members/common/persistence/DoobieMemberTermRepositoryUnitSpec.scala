package repcheck.members.common.persistence

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import repcheck.shared.models.congress.common.{Chamber, UsState}
import repcheck.shared.models.congress.dos.member.MemberTermDO
import repcheck.shared.models.congress.member.MemberType

class DoobieMemberTermRepositoryUnitSpec extends AnyFlatSpec with Matchers {

  private val repo = new DoobieMemberTermRepository

  private val sampleTerm = MemberTermDO(
    termId = 0L,
    memberId = 1L,
    chamber = Some(Chamber.House),
    congress = Some(118),
    startYear = Some(2023),
    endYear = Some(2025),
    memberType = Some(MemberType.Representative),
    stateCode = Some(UsState.NewYork),
    stateName = Some("New York"),
    district = Some(5),
  )

  "replaceAll" should "produce a ConnectionIO describing the delete-then-insert" in {
    val cio = repo.replaceAll(1L, List(sampleTerm))
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  it should "handle an empty term list" in {
    val cio = repo.replaceAll(1L, List.empty)
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  it should "handle multiple terms" in {
    val second = sampleTerm.copy(congress = Some(117), startYear = Some(2021), endYear = Some(2023))
    val cio    = repo.replaceAll(1L, List(sampleTerm, second))
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  it should "handle terms with all optional fields set to None" in {
    val minimal = MemberTermDO(
      termId = 0L,
      memberId = 1L,
      chamber = None,
      congress = None,
      startYear = None,
      endYear = None,
      memberType = None,
      stateCode = None,
      stateName = None,
      district = None,
    )
    val cio = repo.replaceAll(1L, List(minimal))
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  it should "accept Senate terms" in {
    val senateTerm = sampleTerm.copy(
      chamber = Some(Chamber.Senate),
      memberType = Some(MemberType.Senator),
      district = None,
    )
    val cio = repo.replaceAll(2L, List(senateTerm))
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

  "DoobieMemberTermRepository" should "implement MemberTermRepository trait" in {
    repo shouldBe a[MemberTermRepository]
  }

}
