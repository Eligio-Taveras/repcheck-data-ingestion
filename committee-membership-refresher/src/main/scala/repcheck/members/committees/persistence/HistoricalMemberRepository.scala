package repcheck.members.committees.persistence

import doobie.ConnectionIO

import repcheck.members.committees.model.HistoricalMemberRow

/** Loads the members who served in a given congress, for resolving CDIR committee-listing names → member ids. */
trait HistoricalMemberRepository {
  def membersForCongress(congress: Int): ConnectionIO[List[HistoricalMemberRow]]
}
