package repcheck.members.committees.persistence

import cats.syntax.all._

import doobie._
import doobie.implicits._
import doobie.postgres.implicits._

import repcheck.members.committees.model.CommitteeMemberDO
import repcheck.pipeline.models.constants.Tables

class DoobieCommitteeMemberRepository extends CommitteeMemberRepository {

  private val table: Fragment = Fragment.const(Tables.CommitteeMembers)

  private val columns: Fragment =
    fr"""id, committee_code, member_id, position, side, rank, congress,
         begin_date, end_date, created_at, updated_at"""

  override def upsert(member: CommitteeMemberDO): ConnectionIO[Unit] =
    sql"""INSERT INTO $table (
            committee_code, member_id, position, side, rank, congress,
            begin_date, end_date
          ) VALUES (
            ${member.committeeCode}, ${member.memberId}, ${member.position},
            ${member.side}, ${member.rank}, ${member.congress},
            ${member.beginDate}, ${member.endDate}
          )
          ON CONFLICT (committee_code, member_id, congress) DO UPDATE SET
            position = EXCLUDED.position,
            side = EXCLUDED.side,
            rank = EXCLUDED.rank,
            begin_date = EXCLUDED.begin_date,
            end_date = EXCLUDED.end_date,
            updated_at = NOW()""".update.run.void

  override def findByCommittee(committeeCode: String): ConnectionIO[List[CommitteeMemberDO]] =
    (fr"SELECT" ++ columns ++ fr"FROM" ++ table ++ fr"WHERE committee_code = $committeeCode")
      .query[CommitteeMemberDO]
      .to[List]

  override def findByMember(memberId: Long): ConnectionIO[List[CommitteeMemberDO]] =
    (fr"SELECT" ++ columns ++ fr"FROM" ++ table ++ fr"WHERE member_id = $memberId")
      .query[CommitteeMemberDO]
      .to[List]

}
