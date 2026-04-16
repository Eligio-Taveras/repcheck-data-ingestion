package repcheck.members.lismapping.repository

import doobie.ConnectionIO

import repcheck.shared.models.congress.dos.member.MemberLisMappingDO

trait LisMappingRepository {
  def upsert(mapping: MemberLisMappingDO): ConnectionIO[UpsertResult]
  def upsertBatch(mappings: List[MemberLisMappingDO]): ConnectionIO[List[UpsertResult]]
  def findByLisMemberId(lisMemberId: Long): ConnectionIO[Option[MemberLisMappingDO]]
  def findByMemberId(memberId: Long): ConnectionIO[Option[MemberLisMappingDO]]
}
