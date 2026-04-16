package repcheck.members.common.persistence

import doobie.ConnectionIO

import repcheck.shared.models.congress.dos.member.MemberTermDO

/**
 * Replace-all repository for a member's term rows. Because a member's terms are fully expressed on every Congress.gov
 * Member.terms[] payload, the caller overwrites the entire set rather than diffing individual rows.
 */
trait MemberTermRepository {
  def replaceAll(memberId: Long, terms: List[MemberTermDO]): ConnectionIO[Unit]
  def findByMemberId(memberId: Long): ConnectionIO[List[MemberTermDO]]
}
