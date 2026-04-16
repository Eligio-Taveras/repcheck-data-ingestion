package repcheck.members.lismapping.repository

import doobie.ConnectionIO

import repcheck.shared.models.congress.dos.member.LisMemberDO

trait LisMemberRepository {
  def upsertByNaturalKey(lisMemberDO: LisMemberDO): ConnectionIO[Long]
  def findByNaturalKey(lisId: String): ConnectionIO[Option[LisMemberDO]]
  def findById(id: Long): ConnectionIO[Option[LisMemberDO]]
}
