package repcheck.ingestion.votes.repo

import doobie._
import doobie.implicits._
import doobie.postgres.implicits._

import repcheck.ingestion.votes.repo.VoteDoobieInstances._
import repcheck.pipeline.models.constants.Tables
import repcheck.shared.models.congress.common.DoobieEnumInstances._
import repcheck.shared.models.congress.dos.vote.VoteDO

/**
 * Doobie implementation of [[VoteRepository]]. Every query lists the `votes` columns explicitly in the order that
 * matches [[VoteDO]]'s constructor parameters: the database-assigned `id` (BIGSERIAL, added by migration 011) moves
 * first, followed by `natural_key` (the former TEXT primary key), then the business columns, and finally the
 * database-managed timestamps `created_at` and `updated_at`. Doobie maps columns positionally; `SELECT *` would return
 * physical-table order, which no longer matches the case-class field order after the PK standardization migration.
 *
 * The enum-valued columns (`chamber`, `vote_type`, `vote_method`, `legislation_type`) are backed by PostgreSQL ENUM
 * types declared in migration 013, so we import `DoobieEnumInstances._` to pick up the `pgEnumStringOpt`-based
 * `Get`/`Put` instances. Without those instances Doobie's auto-derivation would fall back to `VARCHAR`, which the
 * server rejects with `column X is of type enum_foo but expression is of type character varying`.
 */
class DoobieVoteRepository extends VoteRepository {

  /**
   * Explicit column list matching [[VoteDO]] constructor parameter order. Must stay in sync with the case class — any
   * added field that lacks a corresponding column here (or in a different position) would cause Doobie to map the wrong
   * column to the wrong field and silently deserialize garbage.
   */
  private val voteColumns: Fragment =
    fr"""id, natural_key, congress, chamber, roll_number, session_number, bill_id,
         question, vote_type, vote_method, result, vote_date, legislation_number,
         legislation_type, legislation_url, source_data_url, update_date,
         created_at, updated_at"""

  override def findByNaturalKey(naturalKey: String): ConnectionIO[Option[VoteDO]] = {
    val table = Fragment.const(Tables.Votes)
    (fr"SELECT" ++ voteColumns ++ fr"FROM" ++ table ++ fr"WHERE natural_key = $naturalKey")
      .query[VoteDO]
      .option
  }

  override def findByBillId(billId: Long): ConnectionIO[List[VoteDO]] = {
    val table = Fragment.const(Tables.Votes)
    (fr"SELECT" ++ voteColumns ++ fr"FROM" ++ table ++ fr"WHERE bill_id = $billId")
      .query[VoteDO]
      .to[List]
  }

  override def findByCongress(congress: Int): ConnectionIO[List[VoteDO]] = {
    val table = Fragment.const(Tables.Votes)
    (fr"SELECT" ++ voteColumns ++ fr"FROM" ++ table ++ fr"WHERE congress = $congress")
      .query[VoteDO]
      .to[List]
  }

  /**
   * Insert-or-update keyed by `natural_key`. `RETURNING` reads back the full row so the caller sees the
   * database-assigned `id` on first insert and the freshly advanced `updated_at` on every overwrite. The UPDATE set
   * intentionally omits `created_at` — rewriting the creation timestamp would erase auditing history.
   *
   * The `RETURNING` list mirrors [[voteColumns]] so Doobie's `Read[VoteDO]` sees fields in the case-class order.
   */
  override def upsert(vote: VoteDO): ConnectionIO[VoteDO] = {
    val table = Fragment.const(Tables.Votes)
    sql"""INSERT INTO $table (
      natural_key, congress, chamber, roll_number, session_number, bill_id,
      question, vote_type, vote_method, result, vote_date, legislation_number,
      legislation_type, legislation_url, source_data_url, update_date
    ) VALUES (
      ${vote.naturalKey}, ${vote.congress}, ${vote.chamber}, ${vote.rollNumber},
      ${vote.sessionNumber}, ${vote.billId}, ${vote.question}, ${vote.voteType},
      ${vote.voteMethod}, ${vote.result}, ${vote.voteDate}, ${vote.legislationNumber},
      ${vote.legislationType}, ${vote.legislationUrl}, ${vote.sourceDataUrl},
      ${vote.updateDate}
    )
    ON CONFLICT (natural_key) DO UPDATE SET
      congress = EXCLUDED.congress,
      chamber = EXCLUDED.chamber,
      roll_number = EXCLUDED.roll_number,
      session_number = EXCLUDED.session_number,
      bill_id = EXCLUDED.bill_id,
      question = EXCLUDED.question,
      vote_type = EXCLUDED.vote_type,
      vote_method = EXCLUDED.vote_method,
      result = EXCLUDED.result,
      vote_date = EXCLUDED.vote_date,
      legislation_number = EXCLUDED.legislation_number,
      legislation_type = EXCLUDED.legislation_type,
      legislation_url = EXCLUDED.legislation_url,
      source_data_url = EXCLUDED.source_data_url,
      update_date = EXCLUDED.update_date,
      updated_at = NOW()
    RETURNING id, natural_key, congress, chamber, roll_number, session_number, bill_id,
              question, vote_type, vote_method, result, vote_date, legislation_number,
              legislation_type, legislation_url, source_data_url, update_date,
              created_at, updated_at""".query[VoteDO].unique
  }

}
