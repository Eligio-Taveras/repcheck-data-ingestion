package repcheck.members.common.persistence

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class MemberTermRepositorySpec extends AnyFlatSpec with Matchers {

  "DoobieMemberTermRepository" should "compile and be instantiable" in {
    val repo: MemberTermRepository = new DoobieMemberTermRepository
    repo.toString should not be empty
  }

  it should "implement MemberTermRepository" in {
    val repo = new DoobieMemberTermRepository
    repo shouldBe a[MemberTermRepository]
  }

}
