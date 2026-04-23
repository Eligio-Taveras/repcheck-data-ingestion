package repcheck.ingestion.votes.pipeline

import cats.effect.Async

import doobie.ConnectionIO
import doobie.implicits._
import doobie.util.transactor.Transactor

import repcheck.ingestion.votes.repo.{VoteHistoryArchiver, VotePositionRepository, VoteRepository}
import repcheck.shared.models.congress.dos.vote.{VoteDO, VotePositionDO}

/**
 * Orchestrates the archive → upsert → positions-replace sequence inside a single `ConnectionIO` transaction per write.
 * The persister is the only place where the parent-child atomicity of a vote and its positions is enforced — and the
 * shape of the API reflects the sequencing explicitly:
 *
 *   - The caller supplies the parent [[VoteDO]] and a factory `Long => List[VotePositionDO]`.
 *   - Inside the transaction the persister upserts the vote first, receives the DB-assigned `voteId`, and only THEN
 *     invokes the factory to build the positions with the real `voteId` (and real `member_id` / `lis_member_id` values
 *     pre-resolved by the caller).
 *
 * No [[VotePositionDO]] is ever carried through the pipeline with a fake `voteId = 0L`. The factory constructs
 * positions with the real `voteId` the moment it becomes available.
 *
 * ==Three entry points mirroring the §6.4 decision matrix==
 *
 *   - [[persistNew]] — no archival (nothing to snapshot); upsert vote + invoke factory + replaceAll positions.
 *   - [[persistUpdate]] — archive stored row first; then upsert + invoke factory + replaceAll. Expected input:
 *     `storedVoteId` is the id of the row being overwritten (known from the processor's stored lookup).
 *   - [[persistMetadataOnlyUpdate]] — archive + upsert; skip `replaceAll` because the change detector reported
 *     `positionsChanged = false` (incoming positions are byte-identical to stored; no need to churn `vote_positions`).
 *
 * All three run as ONE `ConnectionIO` transaction. A mid-sequence failure rolls the whole write back.
 */
class VotePersister[F[_]: Async](
  voteRepo: VoteRepository,
  positionRepo: VotePositionRepository,
  historyArchiver: VoteHistoryArchiver,
  xa: Transactor[F],
) {

  /**
   * Insert a brand-new vote and its position list. No archival because there is no prior version to snapshot.
   * `buildPositions` is called with the upserted vote's DB-assigned `voteId`, inside the transaction, so the positions
   * are built exactly once with the real `voteId`.
   */
  def persistNew(voteDo: VoteDO, buildPositions: Long => List[VotePositionDO]): F[VoteDO] = {
    val program = for {
      persisted <- voteRepo.upsert(voteDo)
      _         <- positionRepo.replaceAll(persisted.voteId, buildPositions(persisted.voteId))
    } yield persisted
    program.transact(xa)
  }

  /**
   * Archive the current live row (parent + children), upsert the new vote metadata, and replace the position set. The
   * caller supplies `storedVoteId` from its own lookup so the archiver knows which row to snapshot; the archived ids
   * are separate from the `voteId` returned by the upsert (which reuses the same BIGSERIAL on conflict).
   * `buildPositions` runs after the upsert with the real `voteId`, same as [[persistNew]].
   */
  def persistUpdate(
    voteDo: VoteDO,
    storedVoteId: Long,
    buildPositions: Long => List[VotePositionDO],
  ): F[VoteDO] = {
    val program = for {
      _         <- historyArchiver.archiveVote(storedVoteId)
      persisted <- voteRepo.upsert(voteDo)
      _         <- positionRepo.replaceAll(persisted.voteId, buildPositions(persisted.voteId))
    } yield persisted
    program.transact(xa)
  }

  /**
   * Archive + upsert without touching positions. Used when the change report is `Updated(positionsChanged = false)` —
   * the vote metadata changed but the positions are identical to stored. We still archive so the prior metadata is
   * preserved in `vote_history`, but we skip the DELETE + INSERT on `vote_positions` because there is nothing new to
   * write.
   */
  def persistMetadataOnlyUpdate(voteDo: VoteDO, storedVoteId: Long): F[VoteDO] = {
    val program: ConnectionIO[VoteDO] = for {
      _         <- historyArchiver.archiveVote(storedVoteId)
      persisted <- voteRepo.upsert(voteDo)
    } yield persisted
    program.transact(xa)
  }

}
