package repcheck.members.committees.persistence

import cats.syntax.all._

import doobie._
import doobie.implicits._
import doobie.postgres.implicits._

import repcheck.members.committees.model.CommitteeDO
import repcheck.pipeline.models.constants.Tables

class DoobieCommitteeRepository extends CommitteeRepository {

  private val table: Fragment = Fragment.const(Tables.Committees)

  private val columns: Fragment =
    fr"""committee_code, name, chamber, committee_type, parent_committee_code,
         is_current, update_date, created_at, updated_at"""

  override def upsert(committee: CommitteeDO): ConnectionIO[Unit] =
    sql"""INSERT INTO $table (
            committee_code, name, chamber, committee_type, parent_committee_code,
            is_current, update_date
          ) VALUES (
            ${committee.committeeCode}, ${committee.name}, ${committee.chamber},
            ${committee.committeeType}, ${committee.parentCommitteeCode},
            ${committee.isCurrent}, ${committee.updateDate}
          )
          ON CONFLICT (committee_code) DO UPDATE SET
            name = EXCLUDED.name,
            chamber = EXCLUDED.chamber,
            committee_type = EXCLUDED.committee_type,
            parent_committee_code = COALESCE(EXCLUDED.parent_committee_code, $table.parent_committee_code),
            is_current = EXCLUDED.is_current,
            update_date = EXCLUDED.update_date,
            updated_at = NOW()""".update.run.void

  override def upsertPlaceholder(committeeCode: String, chamber: String): ConnectionIO[Unit] =
    sql"""INSERT INTO $table (committee_code, name, chamber, committee_type, is_current)
          VALUES ($committeeCode, $committeeCode, $chamber, 'Unknown', true)
          ON CONFLICT (committee_code) DO NOTHING""".update.run.void

  override def findByCode(committeeCode: String): ConnectionIO[Option[CommitteeDO]] =
    (fr"SELECT" ++ columns ++ fr"FROM" ++ table ++ fr"WHERE committee_code = $committeeCode")
      .query[CommitteeDO]
      .option

  override def findAllSenateParentCodes(): ConnectionIO[List[String]] =
    (fr"SELECT committee_code FROM" ++ table ++
      fr"WHERE chamber = 'Senate' AND parent_committee_code IS NULL AND committee_type != 'Subcommittee'")
      .query[String]
      .to[List]

  override def setParent(childCode: String, parentCode: String): ConnectionIO[Unit] =
    sql"""UPDATE $table SET parent_committee_code = $parentCode, updated_at = NOW()
          WHERE committee_code = $childCode AND parent_committee_code IS NULL""".update.run.void

}
