package repcheck.members.common.persistence

import cats.effect.unsafe.implicits.global

import doobie.implicits._

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import repcheck.members.common.testing.{DockerRequired, TransactorFixture}
import repcheck.shared.models.congress.common.{Chamber, UsState}
import repcheck.shared.models.congress.dos.member.MemberTermDO
import repcheck.shared.models.congress.member.MemberType

class DoobieMemberTermRepositorySpec extends AnyFlatSpec with Matchers with TransactorFixture {

  private lazy val repo = new DoobieMemberTermRepository

  private def makeTerm(memberId: Long, congress: Int, startYear: Int): MemberTermDO =
    MemberTermDO(
      termId = 0L,
      memberId = memberId,
      chamber = Some(Chamber.House),
      congress = Some(congress),
      startYear = Some(startYear),
      endYear = Some(startYear + 2),
      memberType = Some(MemberType.Representative),
      stateCode = Some(UsState.NewYork),
      stateName = Some("New York"),
      district = Some(5),
    )

  "replaceAll" should "insert terms for a newly-seeded member" taggedAs DockerRequired in {
    val memberId = insertMember("T000001")
    val terms    = List(makeTerm(memberId, 117, 2021), makeTerm(memberId, 118, 2023))

    repo.replaceAll(memberId, terms).transact(xa).unsafeRunSync()
    val found = repo.findByMemberId(memberId).transact(xa).unsafeRunSync()
    found.size shouldBe 2
  }

  it should "replace existing terms on re-ingest" taggedAs DockerRequired in {
    val memberId = insertMember("T000002")
    repo
      .replaceAll(memberId, List(makeTerm(memberId, 116, 2019), makeTerm(memberId, 117, 2021)))
      .transact(xa)
      .unsafeRunSync()

    repo
      .replaceAll(memberId, List(makeTerm(memberId, 118, 2023)))
      .transact(xa)
      .unsafeRunSync()

    val found = repo.findByMemberId(memberId).transact(xa).unsafeRunSync()
    val _     = found.size shouldBe 1
    found.headOption.flatMap(_.congress) shouldBe Some(118)
  }

  it should "handle empty list (clears all terms)" taggedAs DockerRequired in {
    val memberId = insertMember("T000003")
    repo.replaceAll(memberId, List(makeTerm(memberId, 117, 2021))).transact(xa).unsafeRunSync()

    repo.replaceAll(memberId, List.empty).transact(xa).unsafeRunSync()

    val found = repo.findByMemberId(memberId).transact(xa).unsafeRunSync()
    found shouldBe empty
  }

  it should "preserve Senate terms with null district" taggedAs DockerRequired in {
    val memberId = insertMember("T000004")
    val senate = MemberTermDO(
      termId = 0L,
      memberId = memberId,
      chamber = Some(Chamber.Senate),
      congress = Some(118),
      startYear = Some(2023),
      endYear = None,
      memberType = Some(MemberType.Senator),
      stateCode = Some(UsState.NewYork),
      stateName = Some("New York"),
      district = None,
    )
    repo.replaceAll(memberId, List(senate)).transact(xa).unsafeRunSync()
    val found = repo.findByMemberId(memberId).transact(xa).unsafeRunSync()
    val _     = found.size shouldBe 1
    found.headOption.flatMap(_.district) shouldBe None
  }

  "findByMemberId" should "return empty list for a member with no terms" taggedAs DockerRequired in {
    val memberId = insertMember("T000005")
    val found    = repo.findByMemberId(memberId).transact(xa).unsafeRunSync()
    found shouldBe empty
  }

  it should "return empty for an unknown member id" taggedAs DockerRequired in {
    val found = repo.findByMemberId(99999L).transact(xa).unsafeRunSync()
    found shouldBe empty
  }

}
