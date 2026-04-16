package repcheck.members.lismapping.repository

import doobie._
import doobie.implicits._
import doobie.postgres.implicits._

import repcheck.pipeline.models.constants.Tables
import repcheck.shared.models.congress.dos.member.LisMemberDO

/**
 * Doobie implementation of [[LisMemberRepository]].
 *
 * Columns are listed explicitly (never `SELECT *`) so that they map positionally to the [[LisMemberDO]] constructor
 * parameter order regardless of the physical column layout in PostgreSQL. The `id` column is BIGSERIAL and is never
 * included in INSERT clauses — the database generates it.
 */
class DoobieLisMemberRepository extends LisMemberRepository {

  private val table = Fragment.const(Tables.LisMembers)

  private val selectColumns: Fragment =
    fr"""id, natural_key, first_name, last_name, party, state, last_verified, created_at"""

  override def upsertByNaturalKey(lisMemberDO: LisMemberDO): ConnectionIO[Long] =
    sql"""
      INSERT INTO $table (
        natural_key, first_name, last_name, party, state, last_verified
      ) VALUES (
        ${lisMemberDO.naturalKey}, ${lisMemberDO.firstName}, ${lisMemberDO.lastName},
        ${lisMemberDO.party}, ${lisMemberDO.state}, ${lisMemberDO.lastVerified}
      )
      ON CONFLICT (natural_key) DO UPDATE SET
        first_name = EXCLUDED.first_name,
        last_name = EXCLUDED.last_name,
        party = EXCLUDED.party,
        state = EXCLUDED.state,
        last_verified = EXCLUDED.last_verified
      RETURNING id
    """.query[Long].unique

  override def findByNaturalKey(lisId: String): ConnectionIO[Option[LisMemberDO]] =
    (fr"SELECT" ++ selectColumns ++ fr"FROM" ++ table ++ fr"WHERE natural_key = $lisId")
      .query[LisMemberDO]
      .option

  override def findById(id: Long): ConnectionIO[Option[LisMemberDO]] =
    (fr"SELECT" ++ selectColumns ++ fr"FROM" ++ table ++ fr"WHERE id = $id")
      .query[LisMemberDO]
      .option

}
