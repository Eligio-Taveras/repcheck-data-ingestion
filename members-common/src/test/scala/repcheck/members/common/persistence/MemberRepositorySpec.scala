package repcheck.members.common.persistence

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class MemberRepositorySpec extends AnyFlatSpec with Matchers {

  "DoobieMemberRepository" should "compile and be instantiable" in {
    val repo: MemberRepository = new DoobieMemberRepository
    repo.toString should not be empty
  }

  it should "implement all MemberRepository methods" in {
    val repo = new DoobieMemberRepository
    repo shouldBe a[MemberRepository]
  }

}
