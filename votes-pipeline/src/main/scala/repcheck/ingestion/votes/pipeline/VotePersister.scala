package repcheck.ingestion.votes.pipeline

import cats.effect.Async

import doobie.ConnectionIO
import doobie.implicits._
import doobie.util.transactor.Transactor

import repcheck.ingestion.votes.repo.{VoteHistoryArchiver, VotePositionRepository, VoteRepository}
import repcheck.shared.models.congress.dos.vote.{VoteDO, VotePositionDO}

/**
 * Orchestrates the archive → upsert → positions-rewrite sequence inside a single `ConnectionIO` transaction per write.
 * The persister is the only place where the parent-child atomicity of a vote and its positions is enforced:
 *
 *   - **Archive before overwrite.** On the Updated branch of [[VoteChangeReport]], the persister calls
 *     [[VoteHistoryArchiver.archiveVote]] first so the about-to-be-replaced `votes` row and every one of its
 *     `vote_positions` children are snapshotted together under a shared `vote_history.id`. If archival fails, the
 *     transaction rolls back and the live data stays intact.
 *   - **Upsert the vote row.** Returns the persisted `VoteDO` including the DB-assigned `voteId` (BIGSERIAL) that must
 *     be propagated into the positions table on the insert path.
 *   - **Replace position set.** [[VotePositionRepository.replaceAll]] deletes every row with the matching `vote_id` and
 *     batch-inserts the incoming list. An empty list is the supported way to clear positions without deleting the
 *     parent vote. The persister rewrites each [[VotePositionDO]]'s `voteId` from `0L` to the upserted vote's id just
 *     before the insert — positions arriving from converters carry `voteId = 0L` as a placeholder because the
 *     converter has no way to know the DB-assigned value.
 *
 * All three operations compose into one `ConnectionIO` and are committed by a single `.transact(xa)` call, so a
 * mid-sequence failure rolls the whole write back.
 *
 * ==Metadata-only update path==
 * When change detection returns `Updated(positionsChanged = false)`, the persister runs archive + upsert but skips
 * `replaceAll`. The stored positions stay in place — no row churn, no writes to `vote_positions` — because the incoming
 * position set is byte-identical to the stored one. `persistMetadataOnlyUpdate` exposes that explicit branch; callers
 * must NOT pass it an incoming position list that could differ from stored state.
 */
private[pipeline] class VotePersister[F[_]: Async](
  voteRepo: VoteRepository,
  positionRepo: VotePositionRepository,
  historyArchiver: VoteHistoryArchiver,
  xa: Transactor[F],
) {

  /**
   * Insert a brand-new vote and its position list. No archival because there is no prior version to snapshot. Returns
   * the persisted vote with `voteId` populated.
   */
  def persistNew(voteDo: VoteDO, positions: List[VotePositionDO]): F[VoteDO] =
    upsertThenReplacePositions(voteDo, positions).transact(xa)

  /**
   * Archive the current live row (parent + children), upsert the new vote metadata, and replace the position set.
   * Expected input: `voteDo` carries the original `voteId` from the stored lookup so the archiver can read the correct
   * rows. (The caller is the change-detector path, which has a `Some(storedDo)` with the live id in hand.)
   */
  def persistUpdate(
    voteDo: VoteDO,
    positions: List[VotePositionDO],
    storedVoteId: Long,
  ): F[VoteDO] = {
    val program = for {
      _         <- historyArchiver.archiveVote(storedVoteId)
      persisted <- upsertThenReplacePositions(voteDo, positions)
    } yield persisted
    program.transact(xa)
  }

  /**
   * Archive + upsert without touching positions. Used when the change report is `Updated(positionsChanged = false)` —
   * the vote metadata changed (new `updateDate`, maybe a corrected result string) but the positions are identical.
   * We still archive so the prior metadata is preserved and audit queries can see the shape of every revision, but we
   * skip the DELETE + INSERT on positions because there is nothing to change.
   */
  def persistMetadataOnlyUpdate(voteDo: VoteDO, storedVoteId: Long): F[VoteDO] = {
    val program = for {
      _         <- historyArchiver.archiveVote(storedVoteId)
      persisted <- voteRepo.upsert(voteDo)
    } yield persisted
    program.transact(xa)
  }

  /**
   * Composed `ConnectionIO` that upserts the vote, rewrites each position's `voteId` to the upserted vote's id, and
   * replaces the position list. Used by both [[persistNew]] and [[persistUpdate]]; the caller wraps it in its own
   * outer `ConnectionIO` if an archive step must run first.
   */
  private def upsertThenReplacePositions(
    voteDo: VoteDO,
    positions: List[VotePositionDO],
  ): ConnectionIO[VoteDO] =
    for {
      persisted <- voteRepo.upsert(voteDo)
      rewired = positions.map(_.copy(voteId = persisted.voteId))
      _ <- positionRepo.replaceAll(persisted.voteId, rewired)
    } yield persisted

}
