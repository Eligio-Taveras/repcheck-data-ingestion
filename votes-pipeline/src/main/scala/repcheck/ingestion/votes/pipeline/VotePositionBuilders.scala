package repcheck.ingestion.votes.pipeline

import repcheck.shared.models.congress.dos.results.UnresolvedVotePosition
import repcheck.shared.models.congress.dos.vote.VotePositionDO

/**
 * Pure helpers that materialize a [[VotePositionDO]] list with the persister's DB-assigned `voteId` and the
 * pre-resolved member identity for each position. Split out from the processor so the materialization step is testable
 * on its own without the rest of the orchestration.
 *
 * The two arms are symmetric: House positions carry `memberSource = Left(bioguide)` and resolve against a `bioguide →
 * members.id` map; Senate positions carry `memberSource = Right(lisNaturalKey)` and resolve against a `lisNaturalKey →
 * lis_members.id` map. Positions pointing at the wrong arm are dropped silently — each builder is called with a
 * pre-partitioned input list in practice.
 */
private[pipeline] object VotePositionBuilders {

  /**
   * Build the House-arm position list with the real `voteId` and the pre-resolved `members.id` for every bioguide.
   * Positions with `memberSource = Right(_)` are dropped.
   */
  def house(
    voteId: Long,
    positions: List[UnresolvedVotePosition],
    memberMap: Map[String, Long],
  ): List[VotePositionDO] =
    positions.flatMap { up =>
      up.memberSource match {
        case Left(bioguide) =>
          memberMap.get(bioguide).map { memberId =>
            VotePositionDO(
              id = 0L,
              voteId = voteId,
              memberId = Some(memberId),
              position = up.voteCast,
              partyAtVote = up.partyAtVote,
              stateAtVote = up.stateAtVote,
              createdAt = None,
              lisMemberId = None,
              voteCastCandidateName = up.voteCastCandidateName,
            )
          }
        case Right(_) => None
      }
    }

  /**
   * Build the Senate-arm position list with the real `voteId` and the pre-resolved `lis_members.id` for every LIS
   * natural key. Positions with `memberSource = Left(_)` are dropped.
   */
  def senate(
    voteId: Long,
    positions: List[UnresolvedVotePosition],
    lisMap: Map[String, Long],
  ): List[VotePositionDO] =
    positions.flatMap { up =>
      up.memberSource match {
        case Right(lisNaturalKey) =>
          lisMap.get(lisNaturalKey).map { lisMemberId =>
            VotePositionDO(
              id = 0L,
              voteId = voteId,
              memberId = None,
              position = up.voteCast,
              partyAtVote = up.partyAtVote,
              stateAtVote = up.stateAtVote,
              createdAt = None,
              lisMemberId = Some(lisMemberId),
              voteCastCandidateName = up.voteCastCandidateName,
            )
          }
        case Left(_) => None
      }
    }

}
