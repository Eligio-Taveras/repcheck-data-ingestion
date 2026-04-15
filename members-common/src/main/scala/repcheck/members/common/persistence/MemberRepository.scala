package repcheck.members.common.persistence

import doobie.ConnectionIO

import repcheck.shared.models.congress.dos.member.MemberDO

trait MemberRepository {
  def findById(memberId: Long): ConnectionIO[Option[MemberDO]]
  def findByBioguideId(bioguideId: String): ConnectionIO[Option[MemberDO]]
  def upsert(member: MemberDO): ConnectionIO[Long]
  def findPlaceholders(): ConnectionIO[List[MemberDO]]
  def existsWithLisMapping(memberId: Long): ConnectionIO[Boolean]
}
