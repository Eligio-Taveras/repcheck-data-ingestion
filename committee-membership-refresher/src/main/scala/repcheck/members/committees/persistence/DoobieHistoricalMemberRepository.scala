package repcheck.members.committees.persistence

import doobie._
import doobie.implicits._

import repcheck.members.committees.model.HistoricalMemberRow
import repcheck.pipeline.models.constants.Tables

class DoobieHistoricalMemberRepository extends HistoricalMemberRepository {

  private val members: Fragment = Fragment.const(Tables.Members)
  private val terms: Fragment   = Fragment.const(Tables.MemberTerms)

  override def membersForCongress(congress: Int): ConnectionIO[List[HistoricalMemberRow]] =
    (fr"SELECT m.first_name, m.last_name, t.state_name, m.id FROM" ++ members ++ fr"m JOIN" ++ terms ++
      fr"t ON t.member_id = m.id WHERE t.congress = $congress")
      .query[HistoricalMemberRow]
      .to[List]

}
