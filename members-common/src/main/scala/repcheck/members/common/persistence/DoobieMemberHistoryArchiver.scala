package repcheck.members.common.persistence

import doobie._
import doobie.free.connection
import doobie.implicits._

import repcheck.pipeline.models.constants.Tables

/**
 * Doobie implementation of [[MemberHistoryArchiver]]. Performs three steps inside a single [[ConnectionIO]]:
 *
 *   1. Look up the live `members` row by `natural_key = bioguideId`. If the member does not exist, return
 *      `ConnectionIO.unit` (no-op).
 *   1. INSERT a row into `member_history` copying all member columns (not the primary key `id` — `member_history.id` is
 *      a separate BIGSERIAL). Use `RETURNING id` to capture the new archive id.
 *   1. INSERT all of the member's current `member_terms` rows into `member_term_history`, tagging each with the archive
 *      id from step 2.
 *
 * The column lists below match the constructor order of [[repcheck.shared.models.congress.dos.member.MemberHistoryDO]]
 * and [[repcheck.shared.models.congress.dos.member.MemberTermHistoryDO]]. No `SELECT *` is used, because physical
 * column order does not match the case-class field order.
 */
class DoobieMemberHistoryArchiver extends MemberHistoryArchiver[ConnectionIO] {

  override def archiveMember(bioguideId: String): ConnectionIO[Unit] = {
    val membersTable   = Fragment.const(Tables.Members)
    val historyTable   = Fragment.const(Tables.MemberHistory)
    val termsTable     = Fragment.const(Tables.MemberTerms)
    val termHistoryTbl = Fragment.const(Tables.MemberTermHistory)

    val existsQuery = sql"SELECT id FROM $membersTable WHERE natural_key = $bioguideId"
      .query[Long]
      .option

    existsQuery.flatMap {
      case None           => connection.unit
      case Some(memberId) => archiveExisting(historyTable, membersTable, termHistoryTbl, termsTable, memberId)
    }
  }

  private def archiveExisting(
    historyTable: Fragment,
    membersTable: Fragment,
    termHistoryTable: Fragment,
    termsTable: Fragment,
    memberId: Long,
  ): ConnectionIO[Unit] =
    for {
      historyId <- sql"""
        INSERT INTO $historyTable (
          member_id, first_name, last_name, direct_order_name, inverted_order_name,
          honorific_name, birth_year, current_party, state, district,
          image_url, image_attribution, official_url, update_date
        )
        SELECT
          id, first_name, last_name, direct_order_name, inverted_order_name,
          honorific_name, birth_year, current_party, state, district,
          image_url, image_attribution, official_url, update_date
        FROM $membersTable
        WHERE id = $memberId
        RETURNING id
      """.query[Long].unique

      _ <- sql"""
        INSERT INTO $termHistoryTable (
          history_id, member_id, chamber, congress, start_year, end_year,
          member_type, state_code, state_name, district
        )
        SELECT
          $historyId, t.member_id, t.chamber, t.congress, t.start_year, t.end_year,
          t.member_type, t.state_code, t.state_name, t.district
        FROM $termsTable t
        WHERE t.member_id = $memberId
      """.update.run
    } yield ()

}
