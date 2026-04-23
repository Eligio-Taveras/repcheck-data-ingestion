package repcheck.ingestion.votes.pipeline

import cats.effect.Async
import cats.syntax.all._

import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}
import repcheck.ingestion.votes.errors.VoteConversionFailed
import repcheck.shared.models.congress.dos.results.VoteConversionResult
import repcheck.shared.models.congress.dto.conversions.VoteConversions.VoteMembersDTOOps
import repcheck.shared.models.congress.dto.vote.VoteMembersDTO

/**
 * Converts a House-side [[VoteMembersDTO]] (already fetched from Congress.gov `/house-vote/.../members`) into a
 * [[VoteConversionResult]] — `VoteDO` + `billNaturalKey` + `List[UnresolvedVotePosition]`. The converter does NOT build
 * [[repcheck.shared.models.congress.dos.vote.VotePositionDO]] rows and does NOT resolve bioguides to `members.id` or
 * bill natural keys to `bills.id`. Those are processor-level concerns that only make sense once the persisted vote's
 * `voteId` is known (for positions) and the caller is ready to run the placeholder+lookup sequence (for members and
 * bills).
 *
 * The resulting shape keeps the conversion purely structural:
 *   - `vote: VoteDO` carries the new vote metadata, with `billId = None` (the processor sets this after resolving
 *     `billNaturalKey` via the bill repository + placeholder creator).
 *   - `billNaturalKey: Option[String]` — derived from the DTO's `legislationType + legislationNumber + congress` when
 *     present; `None` for procedural votes (no legislation reference).
 *   - `positions: List[UnresolvedVotePosition]` — one entry per House voter, with `memberSource = Left(bioguide)`. No
 *     DB-assigned ids (no `voteId`, no `memberId`) are present; the processor resolves bioguides and materializes the
 *     final `VotePositionDO` rows inside the persister's transaction once the parent vote's `voteId` is known.
 *
 * Delegates the pure DTO→DO validation + enum parsing to
 * [[repcheck.shared.models.congress.dto.conversions.VoteConversions.VoteMembersDTOOps.toDO]], passing a no-op
 * `billLookup` (always `F.pure(None)`) because bill resolution happens in the processor, not here. The resulting
 * `VoteDO.billId` is always `None` in the converter's output; the processor overwrites it with the resolved id.
 */
private[pipeline] class HouseVoteConverter[F[_]: Async](logger: PipelineLogger[F]) {

  /**
   * Convert a single `VoteMembersDTO` into a [[VoteConversionResult]]. Raises [[VoteConversionFailed]] when the DTO
   * fails validation (bad congress, missing session, unparseable enum value, etc.).
   */
  def convert(dto: VoteMembersDTO, logCtx: LogContext): F[VoteConversionResult] = {
    val noopBillLookup: String => F[Option[Long]] = _ => Async[F].pure(None)

    for {
      attempt <- dto.toDO(noopBillLookup)
      converted <- attempt match {
        case Right(result) => Async[F].pure(result)
        case Left(reason) =>
          val voteKey = buildNaturalKey(dto)
          val err     = VoteConversionFailed(voteKey, reason)
          logger.error(logCtx, err.getMessage, Some(err)) *>
            Async[F].raiseError[VoteConversionResult](err)
      }
    } yield converted
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

}
