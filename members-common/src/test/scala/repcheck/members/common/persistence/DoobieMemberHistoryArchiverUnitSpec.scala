package repcheck.members.common.persistence

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class DoobieMemberHistoryArchiverUnitSpec extends AnyFlatSpec with Matchers {

  private val archiver = new DoobieMemberHistoryArchiver

  "archiveMember" should "produce a ConnectionIO for a valid bioguide id" in {
    val cio = archiver.archiveMember("A000001")
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  it should "produce a ConnectionIO for an empty bioguide id" in {
    val cio = archiver.archiveMember("")
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  it should "produce a ConnectionIO for a bioguide id with special characters" in {
    val cio = archiver.archiveMember("Z-!@#$")
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  "DoobieMemberHistoryArchiver" should "implement MemberHistoryArchiver" in {
    archiver shouldBe a[MemberHistoryArchiver[?]]
  }

}
