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
 * [[VoteConversionResult]] — `VoteDO` + `billNaturalKey` + `List[UnresolvedVotePosition]`.
 *
 * ==Bill resolution==
 *
 * The caller supplies `billLookup: String => F[Option[Long]]` per-call. The converter threads it straight into
 * [[repcheck.shared.models.congress.dto.conversions.VoteConversions.VoteMembersDTOOps.toDO]], which calls it exactly
 * once when the DTO carries legislation fields (bill-linked votes) and skips it entirely for procedural votes. The
 * returned `VoteConversionResult.vote.billId` is the real, resolved `bills.id` — NOT a no-op `None` that the processor
 * has to overwrite later. This is the shared-models API's design: the lookup happens at conversion time so the DO is
 * fully populated once the converter returns.
 *
 * Typically the processor supplies `billLookup` as a small composition of `PlaceholderCreator.ensureExists[BillDO]` +
 * `BillRepository.findByBillId` wrapped in a transactor — so the lookup creates the bill row if missing (to be enriched
 * by the bills-pipeline on its next run), then fetches the new or existing surrogate id. See `VoteProcessor.billLookup`
 * for the canonical wiring.
 *
 * ==Positions==
 *
 * Positions come back as `List[UnresolvedVotePosition]` with `memberSource = Left(bioguide)`. The converter does NOT
 * build [[repcheck.shared.models.congress.dos.vote.VotePositionDO]] rows and does NOT resolve bioguides to `members.id`
 * — those are processor-level concerns that only make sense once the persisted vote's `voteId` is known (the persister
 * calls a factory lambda inside its transaction with the real `voteId`).
 */
class HouseVoteConverter[F[_]: Async](logger: PipelineLogger[F]) {

  /**
   * Convert a single `VoteMembersDTO` into a [[VoteConversionResult]], performing an inline bill lookup via
   * `billLookup` when the DTO carries legislation fields. Raises [[VoteConversionFailed]] when the DTO fails validation
   * (bad congress, missing session, unparseable enum value, etc.). A `billLookup` failure bubbles up to the caller
   * without wrapping — the processor's `billLookup` already raises typed `BillResolutionFailed` when the placeholder
   * round-trip misbehaves, and doubling the wrap would obscure the underlying error.
   */
  def convert(
    dto: VoteMembersDTO,
    billLookup: String => F[Option[Long]],
    logCtx: LogContext,
  ): F[VoteConversionResult] =
    for {
      attempt <- dto.toDO(billLookup)
      converted <- attempt match {
        case Right(result) => Async[F].pure(result)
        case Left(reason) =>
          val voteKey = buildNaturalKey(dto)
          val err     = VoteConversionFailed(voteKey, reason)
          logger.error(logCtx, err.getMessage, Some(err)) *>
            Async[F].raiseError[VoteConversionResult](err)
      }
    } yield converted

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
