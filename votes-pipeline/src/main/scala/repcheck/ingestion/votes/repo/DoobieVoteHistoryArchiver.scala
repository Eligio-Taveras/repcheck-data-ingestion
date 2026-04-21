package repcheck.ingestion.votes.repo

import cats.syntax.all._

import doobie._
import doobie.free.connection
import doobie.implicits._

import repcheck.pipeline.models.constants.Tables

/**
 * Doobie implementation of [[VoteHistoryArchiver]]. Performs three steps inside a single [[ConnectionIO]]:
 *
 *   1. Look up the live `votes` row by primary key. If the vote does not exist, return `0L` (no-op — the caller's
 *      transaction continues uninterrupted).
 *   1. INSERT a row into `vote_history` copying every business column from the live `votes` row (not the primary key
 *      `id` — `vote_history.id` is a separate BIGSERIAL). `RETURNING id` captures the new history id.
 *   1. INSERT every `vote_positions` row into `vote_history_positions`, tagging each with the history id from step 2.
 *
 * Both INSERT-FROM-SELECT statements list columns explicitly in the order matching
 * [[repcheck.shared.models.congress.dos.vote.VoteHistoryDO]] and
 * [[repcheck.shared.models.congress.dos.vote.VoteHistoryPositionDO]] constructors. We deliberately do NOT use `SELECT
 * *` because physical-table column order does not match the case-class field order (the `id` BIGSERIAL was added via
 * ALTER TABLE by migration 011 and sits at the end of the physical row).
 */
class DoobieVoteHistoryArchiver extends VoteHistoryArchiver {

  override def archiveVote(voteId: Long): ConnectionIO[Long] = {
    val votesTable            = Fragment.const(Tables.Votes)
    val historyTable          = Fragment.const(Tables.VoteHistory)
    val positionsTable        = Fragment.const(Tables.VotePositions)
    val historyPositionsTable = Fragment.const(Tables.VoteHistoryPositions)

    val existsQuery =
      (fr"SELECT id FROM" ++ votesTable ++ fr"WHERE id = $voteId")
        .query[Long]
        .option

    existsQuery.flatMap(dispatchArchive(_, historyTable, votesTable, historyPositionsTable, positionsTable))
  }

  /**
   * Branches on whether the live vote exists. A missing vote returns `0L` so the composed `ConnectionIO` stays
   * well-typed without forcing the caller to handle an `Option`. The alternative — `ConnectionIO[Option[Long]]` — would
   * push optionality into every caller for a case they usually don't care about.
   */
  private[repo] def dispatchArchive(
    existingId: Option[Long],
    historyTable: Fragment,
    votesTable: Fragment,
    historyPositionsTable: Fragment,
    positionsTable: Fragment,
  ): ConnectionIO[Long] = existingId match {
    case None         => connection.pure(0L)
    case Some(voteId) => archiveExisting(historyTable, votesTable, historyPositionsTable, positionsTable, voteId)
  }

  private[repo] def archiveExisting(
    historyTable: Fragment,
    votesTable: Fragment,
    historyPositionsTable: Fragment,
    positionsTable: Fragment,
    voteId: Long,
  ): ConnectionIO[Long] =
    insertVoteHistory(historyTable, votesTable, voteId).flatTap(
      archivePositions(historyPositionsTable, positionsTable, voteId)
    )

  private[repo] def archivePositions(
    historyPositionsTable: Fragment,
    positionsTable: Fragment,
    voteId: Long,
  )(historyId: Long): ConnectionIO[Unit] =
    insertHistoryPositions(historyId, historyPositionsTable, positionsTable, voteId).void

  /**
   * Copies the live `votes` row into `vote_history`. The SELECT list order MUST match the INSERT list order — Doobie is
   * not involved at this layer because we're doing an INSERT-SELECT, but the SQL planner would silently map columns by
   * position regardless of naming.
   */
  private[repo] def insertVoteHistory(
    historyTable: Fragment,
    votesTable: Fragment,
    voteId: Long,
  ): ConnectionIO[Long] =
    sql"""
      INSERT INTO $historyTable (
        vote_id, congress, chamber, roll_number, session_number, bill_id,
        question, vote_type, vote_method, result, vote_date, legislation_number,
        legislation_type, legislation_url, source_data_url, update_date
      )
      SELECT
        id, congress, chamber, roll_number, session_number, bill_id,
        question, vote_type, vote_method, result, vote_date, legislation_number,
        legislation_type, legislation_url, source_data_url, update_date
      FROM $votesTable
      WHERE id = $voteId
      RETURNING id
    """.query[Long].unique

  /**
   * Copies every `vote_positions` row for `voteId` into `vote_history_positions`, tagging each with the supplied
   * `historyId`. Returns the number of inserted rows so callers can log it if useful.
   */
  private[repo] def insertHistoryPositions(
    historyId: Long,
    historyPositionsTable: Fragment,
    positionsTable: Fragment,
    voteId: Long,
  ): ConnectionIO[Int] =
    sql"""
      INSERT INTO $historyPositionsTable (
        history_id, member_id, position, party_at_vote, state_at_vote
      )
      SELECT
        $historyId, vp.member_id, vp.position, vp.party_at_vote, vp.state_at_vote
      FROM $positionsTable vp
      WHERE vp.vote_id = $voteId
    """.update.run

}
