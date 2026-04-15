package repcheck.members.common.persistence

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import doobie.util.Write

import repcheck.shared.models.congress.dos.member.MemberDO

class MemberWriteInstancesSpec extends AnyFlatSpec with Matchers {

  "memberInsertWrite" should "provide a Write instance for MemberDO" in {
    val writeInstance: Write[MemberDO] = MemberWriteInstances.memberInsertWrite
    writeInstance.toString should not be empty
  }

  it should "produce the correct number of columns for the insert" in {
    val writeInstance: Write[MemberDO] = MemberWriteInstances.memberInsertWrite
    // Write instance should have 14 columns matching the INSERT statement
    // (naturalKey through updateDate, excluding memberId, createdAt, updatedAt)
    writeInstance.length shouldBe 14
  }

}
