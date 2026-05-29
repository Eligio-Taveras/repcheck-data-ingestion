package repcheck.members.committees.persistence

import cats.syntax.all._

import doobie._
import doobie.implicits._
import doobie.postgres.implicits._

import repcheck.members.committees.model.{CommitteeDO, CommitteeInsert}
import repcheck.pipeline.models.constants.Tables

class DoobieCommitteeRepository extends CommitteeRepository {

  private val table: Fragment = Fragment.const(Tables.Committees)

  private val selectColumns: Fragment =
    fr"""id, natural_key, name, chamber, committee_type::text,
         parent_committee_id, url, update_date, is_current, created_at, updated_at"""

  private val returningColumns: Fragment =
    fr"""RETURNING id, natural_key, name, chamber, committee_type::text,
         parent_committee_id, url, update_date, is_current, created_at, updated_at"""

  override def upsert(committee: CommitteeInsert): ConnectionIO[CommitteeDO] =
    (sql"""INSERT INTO $table (
            natural_key, name, chamber, committee_type, parent_committee_id,
            url, update_date, is_current
          ) VALUES (
            ${committee.naturalKey}, ${committee.name}, ${committee.chamber}::chamber_type,
            ${committee.committeeType}::committee_type_enum, ${committee.parentCommitteeId},
            ${committee.url}, ${committee.updateDate}, ${committee.isCurrent}
          )
          ON CONFLICT (natural_key) DO UPDATE SET
            name = EXCLUDED.name,
            chamber = EXCLUDED.chamber,
            committee_type = EXCLUDED.committee_type,
            parent_committee_id = COALESCE(EXCLUDED.parent_committee_id, committees.parent_committee_id),
            url = EXCLUDED.url,
            update_date = EXCLUDED.update_date,
            is_current = EXCLUDED.is_current,
            updated_at = NOW()
          """ ++ returningColumns)
      .query[CommitteeDO]
      .unique

  override def upsertPlaceholder(naturalKey: String, chamber: String): ConnectionIO[CommitteeDO] =
    (sql"""INSERT INTO $table (natural_key, name, chamber, is_current)
          VALUES ($naturalKey, $naturalKey, $chamber::chamber_type, true)
          ON CONFLICT (natural_key) DO UPDATE SET
            updated_at = NOW()
          """ ++ returningColumns)
      .query[CommitteeDO]
      .unique

  override def findByCode(naturalKey: String): ConnectionIO[Option[CommitteeDO]] =
    (fr"SELECT" ++ selectColumns ++ fr"FROM" ++ table ++ fr"WHERE natural_key = $naturalKey")
      .query[CommitteeDO]
      .option

  override def listAll(): ConnectionIO[List[CommitteeDO]] =
    (fr"SELECT" ++ selectColumns ++ fr"FROM" ++ table)
      .query[CommitteeDO]
      .to[List]

  override def setParent(childCode: String, parentId: Long): ConnectionIO[Unit] =
    sql"""UPDATE $table SET parent_committee_id = $parentId, updated_at = NOW()
          WHERE natural_key = $childCode AND parent_committee_id IS NULL""".update.run.void

  override def countCurrent(): ConnectionIO[Int] =
    (fr"SELECT COUNT(*) FROM" ++ table ++ fr"WHERE is_current = true")
      .query[Int]
      .unique

}
