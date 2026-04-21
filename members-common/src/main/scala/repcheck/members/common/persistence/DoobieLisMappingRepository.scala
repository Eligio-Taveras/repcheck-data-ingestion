package repcheck.members.common.persistence

import cats.data.NonEmptyList
import cats.syntax.all._

import doobie._
import doobie.implicits._
import doobie.postgres.implicits._
import doobie.util.fragments.in

import repcheck.pipeline.models.constants.Tables
import repcheck.shared.models.congress.dos.member.MemberLisMappingDO

/**
 * Doobie implementation of [[LisMappingRepository]].
 *
 * Uses PostgreSQL's `xmax` system column to distinguish a freshly inserted row from one that was updated via `ON
 * CONFLICT`: `xmax = 0` indicates a brand-new insert, any non-zero value indicates the row already existed and was
 * updated. This distinction drives downstream event emission.
 *
 * `id` is BIGSERIAL and never appears in INSERT clauses.
 *
 * Schema note: `member_id` and `lis_member_id` are both BIGINT surrogate keys, not the Congress.gov string identifiers.
 * `member_id` is a FK to `members.id` (whose natural key is the bioguide id, e.g. `"P000001"`); `lis_member_id` is a FK
 * to `lis_members.id` (whose natural key is the Senate LIS code, e.g. `"S428"`). Both natural keys live on their parent
 * tables' `natural_key` columns and are resolved to BIGINT ids before reaching this layer.
 */
class DoobieLisMappingRepository extends LisMappingRepository {

  private val table = Fragment.const(Tables.MemberLisMapping)

  // Column order below matches MemberLisMappingDO's constructor (id, memberId: Long, lisMemberId: Long, lastVerified).
  // member_id / lis_member_id are BIGINT FKs — see class Scaladoc.
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

  /**
   * Issues a single `SELECT lis_member_id, member_id FROM member_lis_mapping WHERE lis_member_id IN (?, ?, ...)` query
   * for any non-empty input, via `doobie.util.fragments.in` over a `NonEmptyList`. Senate roll calls are bounded at
   * ~100 positions, well below PostgreSQL's 65535-parameter limit, so the straightforward IN-list is both portable and
   * index-friendly.
   *
   * Empty input short-circuits with `Map.empty` and no database interaction.
   */
  override def findByLisMemberIds(lisMemberIds: List[Long]): ConnectionIO[Map[Long, Long]] =
    NonEmptyList.fromList(lisMemberIds) match {
      case None =>
        doobie.free.connection.pure(Map.empty[Long, Long])
      case Some(nel) =>
        (fr"SELECT lis_member_id, member_id FROM" ++ table ++ fr"WHERE" ++ in(fr"lis_member_id", nel))
          .query[(Long, Long)]
          .to[List]
          .map(_.toMap)
    }

}
