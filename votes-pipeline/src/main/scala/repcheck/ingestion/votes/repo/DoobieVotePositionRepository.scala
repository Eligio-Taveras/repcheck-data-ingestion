package repcheck.ingestion.votes.repo

import cats.syntax.all._

import doobie._
import doobie.free.connection
import doobie.implicits._
import doobie.postgres.implicits._

import repcheck.ingestion.votes.errors.VotePositionIdentityInvalid
import repcheck.pipeline.models.constants.Tables
import repcheck.shared.models.congress.common.DoobieEnumInstances._
import repcheck.shared.models.congress.common.{Party, UsState}
import repcheck.shared.models.congress.dos.vote.VotePositionDO
import repcheck.shared.models.congress.vote.VoteCast

/**
 * Doobie implementation of [[VotePositionRepository]]. Migration 023's dual-identity schema means every query must
 * carry `member_id` AND `lis_member_id` columns — exactly one is non-NULL per row, enforced by the DB
 * `chk_vp_xor_identity` CHECK. Column order on every query matches [[VotePositionDO]] constructor order (with
 * `lis_member_id` trailing after `created_at`, mirroring migration 023's `ALTER TABLE ADD COLUMN`).
 *
 * `position`, `party_at_vote`, and `state_at_vote` are enum-backed columns (migrations 013 + 014), so we import
 * `DoobieEnumInstances._` to bring the appropriate `Get`/`Put` instances into scope.
 */
class DoobieVotePositionRepository extends VotePositionRepository {

  /**
   * Explicit column list matching [[VotePositionDO]] constructor parameter order. Order: `id`, `vote_id`, `member_id`,
   * `position`, `party_at_vote`, `state_at_vote`, `created_at`, `lis_member_id`, `vote_cast_candidate_name`. Migrations
   * 023 + 025 each added a column at the end of the physical row via `ALTER TABLE ADD COLUMN`; the DO mirrors that
   * ordering so Doobie's auto-derived `Read`/`Write` align positionally. `SELECT *` would still work today but would
   * silently misalign if any future migration injects a column between existing ones — list columns explicitly.
   */
  private val positionColumns: Fragment =
    fr"id, vote_id, member_id, position, party_at_vote, state_at_vote, created_at, lis_member_id, vote_cast_candidate_name"

  override def findByVoteId(voteId: Long): ConnectionIO[List[VotePositionDO]] = {
    val table = Fragment.const(Tables.VotePositions)
    (fr"SELECT" ++ positionColumns ++ fr"FROM" ++ table ++ fr"WHERE vote_id = $voteId")
      .query[VotePositionDO]
      .to[List]
  }

  override def findByVoteAndMember(voteId: Long, memberId: Long): ConnectionIO[Option[VotePositionDO]] = {
    val table = Fragment.const(Tables.VotePositions)
    (fr"SELECT" ++ positionColumns ++ fr"FROM" ++ table ++
      fr"WHERE vote_id = $voteId AND member_id = $memberId")
      .query[VotePositionDO]
      .option
  }

  override def findByVoteAndLisMember(voteId: Long, lisMemberId: Long): ConnectionIO[Option[VotePositionDO]] = {
    val table = Fragment.const(Tables.VotePositions)
    (fr"SELECT" ++ positionColumns ++ fr"FROM" ++ table ++
      fr"WHERE vote_id = $voteId AND lis_member_id = $lisMemberId")
      .query[VotePositionDO]
      .option
  }

  /**
   * DELETE + batch INSERT in a single `ConnectionIO`. The sequence is fixed (delete first, insert second) so the
   * operation is composable under the caller's outer transaction: if the INSERT fails, the DELETE is rolled back with
   * it. `connection.unit` short-circuits the INSERT for an empty input list — the caller still gets the DELETE.
   *
   * Validates every input position against the XOR invariant before issuing the INSERT. A violation short-circuits the
   * whole `ConnectionIO` with [[VotePositionIdentityInvalid]]; the outer transaction rolls back cleanly.
   */
  override def replaceAll(voteId: Long, positions: List[VotePositionDO]): ConnectionIO[Unit] = {
    val table = Fragment.const(Tables.VotePositions)
    validatePositions(voteId, positions).flatMap { _ =>
      deletePositions(table, voteId).flatMap(_ => insertPositions(voteId, positions))
    }
  }

  /**
   * Scan the input list and raise [[VotePositionIdentityInvalid]] on the first row that violates the XOR identity
   * invariant (exactly one of `memberId`/`lisMemberId` is `Some`). Returns `ConnectionIO.unit` when every row is valid.
   * Scoped `private[repo]` so tests can exercise the validation branches directly.
   */
  private[repo] def validatePositions(
    voteId: Long,
    positions: List[VotePositionDO],
  ): ConnectionIO[Unit] =
    positions.find(p => !isValidIdentity(p)) match {
      case Some(bad) =>
        connection.raiseError[Unit](
          VotePositionIdentityInvalid(voteId, bad.memberId, bad.lisMemberId)
        )
      case None =>
        connection.unit
    }

  /** Returns `true` iff exactly one of `memberId` / `lisMemberId` is populated. */
  private[repo] def isValidIdentity(p: VotePositionDO): Boolean =
    (p.memberId, p.lisMemberId) match {
      case (Some(_), None) => true
      case (None, Some(_)) => true
      case _               => false
    }

  private[repo] def deletePositions(table: Fragment, voteId: Long): ConnectionIO[Int] =
    (fr"DELETE FROM" ++ table ++ fr"WHERE vote_id = $voteId").update.run

  /**
   * Batch-inserts every [[VotePositionDO]] in `positions` under the supplied `voteId`. The DO's own `voteId` is
   * intentionally ignored in favor of the caller-provided argument, so `replaceAll` cannot silently associate positions
   * with the wrong vote.
   *
   * Each position contributes `memberId` and `lisMemberId` as nullable columns — exactly one is populated per row per
   * the XOR invariant already validated by `validatePositions`. The INSERT column list order matches the tuple
   * positional binding so Doobie writes the columns correctly.
   */
  private[repo] def insertPositions(
    voteId: Long,
    positions: List[VotePositionDO],
  ): ConnectionIO[Unit] =
    if (positions.isEmpty) {
      connection.unit
    } else {
      val insertSql = s"""INSERT INTO ${Tables.VotePositions}
        (vote_id, member_id, position, party_at_vote, state_at_vote, lis_member_id, vote_cast_candidate_name)
        VALUES (?, ?, ?, ?, ?, ?, ?)"""
      type Row =
        (Long, Option[Long], Option[VoteCast], Option[Party], Option[UsState], Option[Long], Option[String])
      val rows: List[Row] =
        positions.map(p =>
          (voteId, p.memberId, p.position, p.partyAtVote, p.stateAtVote, p.lisMemberId, p.voteCastCandidateName)
        )
      Update[Row](insertSql).updateMany(rows).void
    }

  /**
   * Dispatch on which identity is populated. PostgreSQL allows only one `ON CONFLICT` target per INSERT, so House-arm
   * rows target `uq_vp_vote_member` (partial UNIQUE on `(vote_id, member_id) WHERE member_id IS NOT NULL`) and
   * Senate-arm rows target `uq_vp_vote_lis` (partial UNIQUE on `(vote_id, lis_member_id) WHERE lis_member_id IS NOT
   * NULL`). A DO that populates neither (or both) identities raises [[VotePositionIdentityInvalid]] before any SQL is
   * issued.
   *
   * The UPDATE set intentionally excludes `created_at` — rewriting the creation timestamp on conflict would erase
   * ingestion history.
   */
  override def upsert(p: VotePositionDO): ConnectionIO[Unit] =
    (p.memberId, p.lisMemberId) match {
      case (Some(_), None) => upsertHouse(p)
      case (None, Some(_)) => upsertSenate(p)
      case _ =>
        connection.raiseError[Unit](
          VotePositionIdentityInvalid(p.voteId, p.memberId, p.lisMemberId)
        )
    }

  /**
   * House-arm upsert: INSERT with `member_id` populated and `lis_member_id` NULL, conflict on the partial UNIQUE
   * `uq_vp_vote_member` (matches when `member_id IS NOT NULL`). Scoped `private[repo]` so tests can reach each branch
   * directly.
   *
   * `created_at` is deliberately omitted from the INSERT column list so the DB-level `DEFAULT NOW()` fires on first
   * insert and is left alone on UPDATE. Sending a NULL explicitly would violate the `NOT NULL` constraint; letting the
   * default take effect keeps callers from having to fabricate a timestamp the DB owns.
   */
  private[repo] def upsertHouse(p: VotePositionDO): ConnectionIO[Unit] = {
    val table = Fragment.const(Tables.VotePositions)
    (fr"INSERT INTO" ++ table ++
      fr"(vote_id, member_id, lis_member_id, position, party_at_vote, state_at_vote, vote_cast_candidate_name)" ++
      fr"VALUES (${p.voteId}, ${p.memberId}, ${Option
          .empty[Long]}, ${p.position}, ${p.partyAtVote}, ${p.stateAtVote}, ${p.voteCastCandidateName})" ++
      fr"ON CONFLICT (vote_id, member_id) WHERE member_id IS NOT NULL" ++
      fr"DO UPDATE SET position = EXCLUDED.position, party_at_vote = EXCLUDED.party_at_vote, state_at_vote = EXCLUDED.state_at_vote, vote_cast_candidate_name = EXCLUDED.vote_cast_candidate_name").update.run.void
  }

  /**
   * Senate-arm upsert: INSERT with `lis_member_id` populated and `member_id` NULL, conflict on the partial UNIQUE
   * `uq_vp_vote_lis` (matches when `lis_member_id IS NOT NULL`). Used for unmapped Senate rows and for mapped Senate
   * rows — both keep the LIS identity on the row itself rather than pre-resolving to `members.id` inside the
   * repository.
   *
   * `created_at` is deliberately omitted from the INSERT column list so the DB-level `DEFAULT NOW()` fires on first
   * insert and is left alone on UPDATE (see upsertHouse for the rationale).
   */
  private[repo] def upsertSenate(p: VotePositionDO): ConnectionIO[Unit] = {
    val table = Fragment.const(Tables.VotePositions)
    (fr"INSERT INTO" ++ table ++
      fr"(vote_id, member_id, lis_member_id, position, party_at_vote, state_at_vote, vote_cast_candidate_name)" ++
      fr"VALUES (${p.voteId}, ${Option
          .empty[Long]}, ${p.lisMemberId}, ${p.position}, ${p.partyAtVote}, ${p.stateAtVote}, ${p.voteCastCandidateName})" ++
      fr"ON CONFLICT (vote_id, lis_member_id) WHERE lis_member_id IS NOT NULL" ++
      fr"DO UPDATE SET position = EXCLUDED.position, party_at_vote = EXCLUDED.party_at_vote, state_at_vote = EXCLUDED.state_at_vote, vote_cast_candidate_name = EXCLUDED.vote_cast_candidate_name").update.run.void
  }

  override def findByMemberAndBill(memberId: Long, billId: Long): ConnectionIO[List[VotePositionDO]] = {
    val positionsTable = Fragment.const(Tables.VotePositions)
    val votesTable     = Fragment.const(Tables.Votes)
    (fr"SELECT vp.id, vp.vote_id, vp.member_id, vp.position, vp.party_at_vote, vp.state_at_vote, vp.created_at, vp.lis_member_id, vp.vote_cast_candidate_name" ++
      fr"FROM" ++ positionsTable ++ fr"vp" ++
      fr"JOIN" ++ votesTable ++ fr"v ON v.id = vp.vote_id" ++
      fr"WHERE vp.member_id = $memberId AND v.bill_id = $billId")
      .query[VotePositionDO]
      .to[List]
  }

}
