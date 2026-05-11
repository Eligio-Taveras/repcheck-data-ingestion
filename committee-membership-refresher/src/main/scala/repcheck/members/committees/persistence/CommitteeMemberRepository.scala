package repcheck.members.committees.persistence

import doobie.ConnectionIO

import repcheck.members.committees.model.CommitteeMemberDO

trait CommitteeMemberRepository {

  def upsert(member: CommitteeMemberDO): ConnectionIO[Unit]

  def findByCommittee(committeeId: Long): ConnectionIO[List[CommitteeMemberDO]]

  def findByMember(memberId: Long): ConnectionIO[List[CommitteeMemberDO]]

}
