package repcheck.members.common.persistence

import doobie.ConnectionIO

import repcheck.shared.models.congress.dos.member.MemberPartyHistoryDO

/**
 * Append-only repository for a member's party affiliation timeline. Unlike [[MemberTermRepository]], party history is
 * append-only: older entries are preserved when a new party change arrives, so `appendNew` deduplicates against the
 * natural key `(member_id, start_year)`.
 */
trait MemberPartyHistoryRepository {
  def appendNew(memberId: Long, entries: List[MemberPartyHistoryDO]): ConnectionIO[Unit]
  def findByMemberId(memberId: Long): ConnectionIO[List[MemberPartyHistoryDO]]
}
