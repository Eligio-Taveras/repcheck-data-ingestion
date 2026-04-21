package repcheck.ingestion.votes.repo

import cats.syntax.all._

import doobie._
import doobie.implicits._
import doobie.postgres.implicits._

import repcheck.pipeline.models.constants.Tables
import repcheck.shared.models.congress.common.DoobieEnumInstances._
import repcheck.shared.models.congress.common.{Party, UsState}
import repcheck.shared.models.congress.dos.vote.VotePositionDO
import repcheck.shared.models.congress.vote.VoteCast

/**
 * Doobie implementation of [[VotePositionRepository]]. `position`, `party_at_vote`, and `state_at_vote` are enum-backed
 * columns (migrations 013 + 014), so we import `DoobieEnumInstances._` to bring the appropriate `Get`/`Put` instances
 * into scope. Column order on every query matches [[VotePositionDO]] constructor order.
 */
class DoobieVotePositionRepository extends VotePositionRepository {

  /**
   * Explicit column list matching [[VotePositionDO]] constructor parameter order. The `vote_positions` table was
   * reshaped by migration 011: a new BIGSERIAL `id` column was added at the physical end of the row for platform
   * consistency, but the case class retains `vote_id` as its first field because that's the semantic PK from the
   * caller's perspective — so we must list columns explicitly rather than relying on `SELECT *`.
   */
  private val positionColumns: Fragment =
    fr"vote_id, member_id, position, party_at_vote, state_at_vote, created_at"

  override def findByVoteId(voteId: Long): ConnectionIO[List[VotePositionDO]] = {
    val table = Fragment.const(Tables.VotePositions)
    (fr"SELECT" ++ positionColumns ++ fr"FROM" ++ table ++ fr"WHERE vote_id = $voteId")
      .query[VotePositionDO]
      .to[List]
  }

  /**
   * DELETE + batch INSERT in a single `ConnectionIO`. The sequence is fixed (delete first, insert second) so the
   * operation is composable under the caller's outer transaction: if the INSERT fails, the DELETE is rolled back with
   * it. `connection.unit` short-circuits the INSERT for an empty input list — the caller still gets the DELETE.
   */
  override def replaceAll(voteId: Long, positions: List[VotePositionDO]): ConnectionIO[Unit] = {
    val table = Fragment.const(Tables.VotePositions)
    deletePositions(table, voteId).flatMap(_ => insertPositions(voteId, positions))
  }

  private[repo] def deletePositions(table: Fragment, voteId: Long): ConnectionIO[Int] =
    (fr"DELETE FROM" ++ table ++ fr"WHERE vote_id = $voteId").update.run

  /**
   * Batch-inserts every [[VotePositionDO]] in `positions` under the supplied `voteId`. The DO's own `voteId` is
   * intentionally ignored in favor of the caller-provided argument, so `replaceAll` cannot silently associate positions
   * with the wrong vote.
   */
  private[repo] def insertPositions(
    voteId: Long,
    positions: List[VotePositionDO],
  ): ConnectionIO[Unit] =
    if (positions.isEmpty) {
      doobie.free.connection.unit
    } else {
      val insertSql = s"""INSERT INTO ${Tables.VotePositions}
        (vote_id, member_id, position, party_at_vote, state_at_vote)
        VALUES (?, ?, ?, ?, ?)"""
      type Row = (Long, Long, Option[VoteCast], Option[Party], Option[UsState])
      val rows: List[Row] = positions.map(p => (voteId, p.memberId, p.position, p.partyAtVote, p.stateAtVote))
      Update[Row](insertSql).updateMany(rows).void
    }

  override def findByMemberAndBill(memberId: Long, billId: Long): ConnectionIO[List[VotePositionDO]] = {
    val positionsTable = Fragment.const(Tables.VotePositions)
    val votesTable     = Fragment.const(Tables.Votes)
    (fr"SELECT vp.vote_id, vp.member_id, vp.position, vp.party_at_vote, vp.state_at_vote, vp.created_at" ++
      fr"FROM" ++ positionsTable ++ fr"vp" ++
      fr"JOIN" ++ votesTable ++ fr"v ON v.id = vp.vote_id" ++
      fr"WHERE vp.member_id = $memberId AND v.bill_id = $billId")
      .query[VotePositionDO]
      .to[List]
  }

}
