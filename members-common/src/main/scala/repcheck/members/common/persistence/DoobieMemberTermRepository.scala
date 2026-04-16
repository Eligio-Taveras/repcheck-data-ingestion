package repcheck.members.common.persistence

import doobie._
import doobie.implicits._

import repcheck.pipeline.models.constants.Tables
import repcheck.shared.models.congress.common.DoobieEnumInstances._
import repcheck.shared.models.congress.common.{Chamber, UsState}
import repcheck.shared.models.congress.dos.member.MemberTermDO
import repcheck.shared.models.congress.member.MemberType

/**
 * Doobie implementation of [[MemberTermRepository]]. `replaceAll` performs a DELETE + batch INSERT inside a single
 * `ConnectionIO`, so the caller composes it into a larger transaction (archive + upsert + term replace + party
 * history). The INSERT column list matches [[MemberTermDO]] field order minus `termId`, which is an auto-generated
 * `BIGSERIAL` populated by the database.
 */
class DoobieMemberTermRepository extends MemberTermRepository {

  private val table = Fragment.const(Tables.MemberTerms)

  private type InsertRow = (
    Long,               // member_id
    Option[Chamber],    // chamber
    Option[Int],        // congress
    Option[Int],        // start_year
    Option[Int],        // end_year
    Option[MemberType], // member_type
    Option[UsState],    // state_code
    Option[String],     // state_name
    Option[Int],        // district
  )

  override def replaceAll(memberId: Long, terms: List[MemberTermDO]): ConnectionIO[Unit] = {
    val delete = sql"DELETE FROM $table WHERE member_id = $memberId".update.run

    val insert = Update[InsertRow](
      s"INSERT INTO ${Tables.MemberTerms} (member_id, chamber, congress, start_year, end_year, member_type, state_code, state_name, district) " +
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
    )

    val rows: List[InsertRow] = terms.map(t =>
      (
        memberId,
        t.chamber,
        t.congress,
        t.startYear,
        t.endYear,
        t.memberType,
        t.stateCode,
        t.stateName,
        t.district,
      )
    )

    for {
      _ <- delete
      _ <- insert.updateMany(rows)
    } yield ()
  }

  override def findByMemberId(memberId: Long): ConnectionIO[List[MemberTermDO]] =
    sql"""SELECT id, member_id, chamber, congress, start_year, end_year,
                 member_type, state_code, state_name, district
          FROM $table
          WHERE member_id = $memberId"""
      .query[MemberTermDO]
      .to[List]

}
