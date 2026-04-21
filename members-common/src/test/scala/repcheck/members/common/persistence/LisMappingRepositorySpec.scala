package repcheck.members.common.persistence

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class LisMappingRepositorySpec extends AnyFlatSpec with Matchers {

  "DoobieLisMappingRepository" should "compile and be instantiable" in {
    val repo: LisMappingRepository = new DoobieLisMappingRepository
    repo.toString should not be empty
  }

  it should "implement all LisMappingRepository methods" in {
    val repo = new DoobieLisMappingRepository
    repo shouldBe a[LisMappingRepository]
  }

}
