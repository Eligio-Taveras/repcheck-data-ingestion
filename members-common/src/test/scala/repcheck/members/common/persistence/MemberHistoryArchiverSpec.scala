package repcheck.members.common.persistence

import doobie.ConnectionIO

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class MemberHistoryArchiverSpec extends AnyFlatSpec with Matchers {

  "DoobieMemberHistoryArchiver" should "compile and be instantiable" in {
    val archiver: MemberHistoryArchiver[ConnectionIO] = new DoobieMemberHistoryArchiver
    archiver.toString should not be empty
  }

  it should "implement MemberHistoryArchiver" in {
    val archiver = new DoobieMemberHistoryArchiver
    archiver shouldBe a[MemberHistoryArchiver[?]]
  }

}
