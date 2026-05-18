package repcheck.members.committees.persistence

import doobie.ConnectionIO

import repcheck.members.committees.model.{CommitteeMemberDO, CommitteeMemberInsert}

trait CommitteeMemberRepository {

  def upsert(member: CommitteeMemberInsert): ConnectionIO[Unit]

  def findByCommittee(committeeId: Long): ConnectionIO[List[CommitteeMemberDO]]

  def findByMember(memberId: Long): ConnectionIO[List[CommitteeMemberDO]]

  def countByCongress(congress: Int): ConnectionIO[Int]

  def countDistinctMembersByCongress(congress: Int): ConnectionIO[Int]

}
