package repcheck.members.lismapping.repository

import cats.syntax.all._

import doobie._
import doobie.implicits._
import doobie.postgres.implicits._

import repcheck.pipeline.models.constants.Tables
import repcheck.shared.models.congress.dos.member.MemberLisMappingDO

/**
 * Doobie implementation of [[LisMappingRepository]].
 *
 * Uses PostgreSQL's `xmax` system column to distinguish a freshly inserted row from one that was updated via
 * `ON CONFLICT`: `xmax = 0` indicates a brand-new insert, any non-zero value indicates the row already existed and was
 * updated. This distinction drives downstream event emission.
 *
 * `id` is BIGSERIAL and never appears in INSERT clauses.
 */
class DoobieLisMappingRepository extends LisMappingRepository {

  private val table = Fragment.const(Tables.MemberLisMapping)

  private val selectColumns: Fragment =
    fr"""id, member_id, lis_member_id, last_verified"""

  override def upsert(mapping: MemberLisMappingDO): ConnectionIO[UpsertResult] =
    sql"""
      INSERT INTO $table (
        member_id, lis_member_id, last_verified
      ) VALUES (
        ${mapping.memberId}, ${mapping.lisMemberId}, ${mapping.lastVerified}
      )
      ON CONFLICT (member_id, lis_member_id) DO UPDATE SET
        last_verified = EXCLUDED.last_verified
      RETURNING (xmax = 0) AS inserted
    """.query[Boolean].unique.map { inserted =>
      if (inserted) {
        UpsertResult.Inserted
      } else {
        UpsertResult.Updated
      }
    }

  override def upsertBatch(mappings: List[MemberLisMappingDO]): ConnectionIO[List[UpsertResult]] =
    mappings.traverse(upsert)

  override def findByLisMemberId(lisMemberId: Long): ConnectionIO[Option[MemberLisMappingDO]] =
    (fr"SELECT" ++ selectColumns ++ fr"FROM" ++ table ++ fr"WHERE lis_member_id = $lisMemberId")
      .query[MemberLisMappingDO]
      .option

  override def findByMemberId(memberId: Long): ConnectionIO[Option[MemberLisMappingDO]] =
    (fr"SELECT" ++ selectColumns ++ fr"FROM" ++ table ++ fr"WHERE member_id = $memberId")
      .query[MemberLisMappingDO]
      .option

}
