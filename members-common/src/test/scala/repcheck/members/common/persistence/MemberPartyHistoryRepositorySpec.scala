package repcheck.members.common.persistence

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class MemberPartyHistoryRepositorySpec extends AnyFlatSpec with Matchers {

  "DoobieMemberPartyHistoryRepository" should "compile and be instantiable" in {
    val repo: MemberPartyHistoryRepository = new DoobieMemberPartyHistoryRepository
    repo.toString should not be empty
  }

  it should "implement MemberPartyHistoryRepository" in {
    val repo = new DoobieMemberPartyHistoryRepository
    repo shouldBe a[MemberPartyHistoryRepository]
  }

}
