package repcheck.ingestion.votes.pipeline

import cats.effect.Async
import cats.syntax.all._

import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}
import repcheck.ingestion.votes.errors.VoteConversionFailed
import repcheck.shared.models.congress.dos.results.UnresolvedVotePosition
import repcheck.shared.models.congress.dos.vote.{VoteDO, VotePositionDO}
import repcheck.shared.models.congress.dto.vote.VoteMembersDTO

/**
 * Converts a House-side `VoteMembersDTO` (already fetched from Congress.gov `/house-vote/.../members`) into a
 * persist-ready `(VoteDO, List[VotePositionDO])` pair.
 *
 * ==Pipeline==
 *   1. Bill resolution (inside `VoteConversions.VoteMembersDTOOps.toDO`): the converter hands `toDO` a `billLookup`
 *      callback that maps the DTO's `legislationType + legislationNumber + congress` into the bill's `bills.id` Long.
 *      `BillResolver.resolve` runs the placeholder-create-if-missing + read-back sequence, so the resolved id reflects
 *      either an already-enriched bill or a placeholder that `bill-metadata-pipeline` will enrich on its next run.
 *      Procedural votes (no legislation) pass `None` through — `VoteDO.billId` stays `None` and downstream scoring
 *      logic can decide whether to consume the event. 2. Pure validation (inside `toDO`): congress/chamber/session
 *      required, every enum-typed field parses or the conversion fails with [[VoteConversionFailed]] carrying the
 *      parser's reason. 3. Member resolution (this class): every House bioguide from the result's `positions:
 *      List[UnresolvedVotePosition]` is batch-resolved via [[MemberResolver]], producing `Map[bioguide, members.id]`.
 *      Each resolution creates a placeholder member row if one doesn't exist, so we always end up with a Long. 4.
 *      Position materialization (this class): each `UnresolvedVotePosition` with `memberSource = Left(bioguide)` is
 *      turned into a `VotePositionDO(memberId = Some(resolvedId), lisMemberId = None, ...)` per the dual-identity
 *      schema's House arm. `voteId` stays `0L` — the upsert path rewrites it after `INSERT RETURNING id`.
 *
 * ==Why the converter does not touch `voteId`==
 * The incoming `VoteDO` already carries `voteId = 0L` (the conversion has no way to know the DB-assigned id). The
 * processor's persister will call `VoteRepository.upsert(voteDo)` first, receive back the `VoteDO` with a real
 * `voteId`, then materialize the positions with that id populated. This class returns positions with `voteId = 0L` as a
 * placeholder; the persister rewrites them.
 */
private[pipeline] class HouseVoteConverter[F[_]: Async](
  memberResolver: MemberResolver[F],
  billResolver: BillResolver[F],
  logger: PipelineLogger[F],
) {

  import repcheck.shared.models.congress.dto.conversions.VoteConversions.VoteMembersDTOOps

  /**
   * Convert a single `VoteMembersDTO` into the (vote, positions) pair ready for persistence. Raises
   * [[VoteConversionFailed]] when validation fails; propagates [[repcheck.ingestion.votes.errors.BillResolutionFailed]]
   * or [[repcheck.ingestion.votes.errors.MemberResolutionFailed]] from the resolvers.
   */
  def convert(dto: VoteMembersDTO, logCtx: LogContext): F[(VoteDO, List[VotePositionDO])] = {
    val billLookup: String => F[Option[Long]] = nk => billResolver.resolve(nk, logCtx).map(Some(_))

    for {
      conversionEither <- dto.toDO(billLookup)
      result <- conversionEither match {
        case Right(cr) => Async[F].pure(cr)
        case Left(reason) =>
          val voteKey = buildNaturalKey(dto)
          val err     = VoteConversionFailed(voteKey, reason)
          logger.error(logCtx, err.getMessage, Some(err)) *>
            Async[F].raiseError[repcheck.shared.models.congress.dos.results.VoteConversionResult](err)
      }
      bioguideIds = result.positions.flatMap(_.memberSource.left.toOption).filter(_.nonEmpty)
      resolvedMap <- memberResolver.resolveBatch(bioguideIds, logCtx)
      positionDOs = materializePositions(result.positions, resolvedMap)
    } yield (result.vote, positionDOs)
  }

  /**
   * Construct the vote natural key directly from the DTO when the processor needs it before conversion succeeds — e.g.,
   * to label a [[VoteConversionFailed]] on the way out. Matches
   * [[repcheck.shared.models.congress.dto.conversions.VoteConversions.buildVoteNaturalKey]] exactly.
   */
  private[pipeline] def buildNaturalKey(dto: VoteMembersDTO): String = {
    val session = dto.sessionNumber.getOrElse(0)
    s"${dto.congress.toString}-${dto.chamber}-${session.toString}-${dto.rollCallNumber.toString}"
  }

  /**
   * Materialize the resolved bioguide list into House-arm `VotePositionDO` rows. Positions with `memberSource =
   * Right(_)` (Senate) are silently dropped — this converter is House-only. Positions whose bioguide is missing from
   * the resolver's output map (shouldn't happen — resolveBatch raises on unresolvable bioguides) are also dropped
   * defensively.
   *
   * `voteId = 0L` is a placeholder; the persister rewrites it after the parent vote's INSERT RETURNING.
   */
  private[pipeline] def materializePositions(
    unresolved: List[UnresolvedVotePosition],
    bioguideToMemberId: Map[String, Long],
  ): List[VotePositionDO] =
    unresolved.flatMap { uvp =>
      uvp.memberSource match {
        case Left(bioguide) =>
          bioguideToMemberId.get(bioguide).map { memberId =>
            VotePositionDO(
              id = 0L,
              voteId = 0L,
              memberId = Some(memberId),
              position = uvp.voteCast,
              partyAtVote = uvp.partyAtVote,
              stateAtVote = uvp.stateAtVote,
              createdAt = None,
              lisMemberId = None,
            )
          }
        case Right(_) => None
      }
    }

}
