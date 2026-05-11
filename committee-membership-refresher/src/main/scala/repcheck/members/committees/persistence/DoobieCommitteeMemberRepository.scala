package repcheck.members.committees.persistence

import cats.syntax.all._

import doobie._
import doobie.implicits._
import doobie.postgres.implicits._

import repcheck.members.committees.model.CommitteeMemberDO
import repcheck.pipeline.models.constants.Tables

class DoobieCommitteeMemberRepository extends CommitteeMemberRepository {

  private val table: Fragment = Fragment.const(Tables.CommitteeMembers)

  private val selectColumns: Fragment =
    fr"""id, committee_id, member_id, role, start_date, end_date,
         side, rank, congress, created_at, updated_at"""

  override def upsert(member: CommitteeMemberDO): ConnectionIO[Unit] =
    sql"""INSERT INTO $table (
            committee_id, member_id, role, start_date, end_date,
            side, rank, congress
          ) VALUES (
            ${member.committeeId}, ${member.memberId}, ${member.role}::committee_position_type,
            ${member.startDate}, ${member.endDate},
            ${member.side}, ${member.rank}, ${member.congress}
          )
          ON CONFLICT (committee_id, member_id, congress) DO UPDATE SET
            role = EXCLUDED.role,
            start_date = EXCLUDED.start_date,
            end_date = EXCLUDED.end_date,
            side = EXCLUDED.side,
            rank = EXCLUDED.rank,
            updated_at = NOW()""".update.run.void

  override def findByCommittee(committeeId: Long): ConnectionIO[List[CommitteeMemberDO]] =
    (fr"SELECT" ++ selectColumns ++ fr"FROM" ++ table ++ fr"WHERE committee_id = $committeeId")
      .query[CommitteeMemberDO]
      .to[List]

  override def findByMember(memberId: Long): ConnectionIO[List[CommitteeMemberDO]] =
    (fr"SELECT" ++ selectColumns ++ fr"FROM" ++ table ++ fr"WHERE member_id = $memberId")
      .query[CommitteeMemberDO]
      .to[List]

}
