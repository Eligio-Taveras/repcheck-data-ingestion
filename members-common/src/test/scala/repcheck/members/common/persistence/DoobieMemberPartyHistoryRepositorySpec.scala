package repcheck.members.common.persistence

import cats.effect.unsafe.implicits.global

import doobie.implicits._

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import repcheck.members.common.testing.{DockerRequired, TransactorFixture}
import repcheck.shared.models.congress.dos.member.MemberPartyHistoryDO

class DoobieMemberPartyHistoryRepositorySpec extends AnyFlatSpec with Matchers with TransactorFixture {

  private lazy val repo = new DoobieMemberPartyHistoryRepository

  private def makeEntry(memberId: Long, startYear: Int, name: String, abbrev: String): MemberPartyHistoryDO =
    MemberPartyHistoryDO(
      id = 0L,
      memberId = memberId,
      partyName = Some(name),
      partyAbbreviation = Some(abbrev),
      startYear = Some(startYear),
    )

  "appendNew" should "insert a single party history entry" taggedAs DockerRequired in {
    val memberId = insertMember("P000001")
    repo.appendNew(memberId, List(makeEntry(memberId, 2005, "Democrat", "D"))).transact(xa).unsafeRunSync()

    val found = repo.findByMemberId(memberId).transact(xa).unsafeRunSync()
    found.size shouldBe 1
  }

  it should "insert multiple entries in a single call" taggedAs DockerRequired in {
    val memberId = insertMember("P000002")
    val entries = List(
      makeEntry(memberId, 2005, "Democrat", "D"),
      makeEntry(memberId, 2011, "Independent", "I"),
      makeEntry(memberId, 2015, "Democrat", "D"),
    )
    repo.appendNew(memberId, entries).transact(xa).unsafeRunSync()

    val found = repo.findByMemberId(memberId).transact(xa).unsafeRunSync()
    found.size shouldBe 3
  }

  it should "deduplicate on repeat calls using (member_id, start_year)" taggedAs DockerRequired in {
    val memberId = insertMember("P000003")
    val first = List(
      makeEntry(memberId, 2005, "Democrat", "D"),
      makeEntry(memberId, 2011, "Independent", "I"),
    )
    repo.appendNew(memberId, first).transact(xa).unsafeRunSync()

    val second = List(
      makeEntry(memberId, 2005, "Democrat", "D"),        // duplicate — ignored
      makeEntry(memberId, 2011, "Independent", "I"),     // duplicate — ignored
      makeEntry(memberId, 2015, "Democrat", "D"),        // new
    )
    repo.appendNew(memberId, second).transact(xa).unsafeRunSync()

    val found = repo.findByMemberId(memberId).transact(xa).unsafeRunSync()
    found.size shouldBe 3
  }

  it should "handle empty list (no rows inserted)" taggedAs DockerRequired in {
    val memberId = insertMember("P000004")
    repo.appendNew(memberId, List.empty).transact(xa).unsafeRunSync()
    val found = repo.findByMemberId(memberId).transact(xa).unsafeRunSync()
    found shouldBe empty
  }

  "findByMemberId" should "return empty for a member with no history entries" taggedAs DockerRequired in {
    val memberId = insertMember("P000005")
    val found    = repo.findByMemberId(memberId).transact(xa).unsafeRunSync()
    found shouldBe empty
  }

  it should "return empty for an unknown member id" taggedAs DockerRequired in {
    val found = repo.findByMemberId(99999L).transact(xa).unsafeRunSync()
    found shouldBe empty
  }

}
