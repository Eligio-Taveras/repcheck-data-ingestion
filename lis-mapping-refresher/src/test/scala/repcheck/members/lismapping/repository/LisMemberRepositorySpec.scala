package repcheck.members.lismapping.repository

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class LisMemberRepositorySpec extends AnyFlatSpec with Matchers {

  "DoobieLisMemberRepository" should "compile and be instantiable" in {
    val repo: LisMemberRepository = new DoobieLisMemberRepository
    repo.toString should not be empty
  }

  it should "implement all LisMemberRepository methods" in {
    val repo = new DoobieLisMemberRepository
    repo shouldBe a[LisMemberRepository]
  }

}
