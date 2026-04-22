package repcheck.ingestion.votes.pipeline

import java.time.{LocalDate, ZoneOffset}
import java.time.format.DateTimeFormatter
import java.util.Locale

import cats.effect.Async
import cats.syntax.all._

import scala.util.Try

import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}
import repcheck.ingestion.votes.errors.VoteConversionFailed
import repcheck.shared.models.congress.common.{Chamber, Party, UsState}
import repcheck.shared.models.congress.dos.vote.{VoteDO, VotePositionDO}
import repcheck.shared.models.congress.dto.conversions.VoteConversions
import repcheck.shared.models.congress.dto.vote.SenateVoteXmlDTO
import repcheck.shared.models.congress.vote.{VoteCast, VoteType}

/**
 * Converts a senate.gov `<roll_call_vote>` XML document (as a decoded [[SenateVoteXmlDTO]]) into a persist-ready
 * `(VoteDO, List[VotePositionDO])` pair, using the `Map[lisNaturalKey, lis_members.id]` produced by
 * [[repcheck.ingestion.votes.lis.LisResolver]] to populate the Senate arm of the dual-identity `vote_positions` schema.
 *
 * ==Why this converter exists separately from `VoteConversions`==
 *
 * The existing `SenateVoteXmlDTOOps.toDO(lisMapping: Map[String, String])` in shared-models predates the migration 023
 * dual-identity design — it shoehorns senate data into a `VoteMembersDTO` where `memberId` holds a bioguide string,
 * which is wrong for positions that should be keyed by `lis_member_id`. This converter supersedes that path for
 * votes-pipeline; the shared-models conversion is effectively dead code and should be removed in a follow-up
 * shared-models bump.
 *
 * ==Output shape==
 *   - `VoteDO.chamber = Chamber.Senate`.
 *   - `VoteDO.billId = None` — senate.gov roll-call XML does not carry bill-linkage metadata. Senate votes about bills
 *     would need a separate enrichment path (out of scope for this pipeline).
 *   - `VoteDO.voteType` classified from the `<question>` via [[VoteType.fromQuestion]] — same rule House uses.
 *   - `VoteDO.voteDate` parsed from the XML's `<vote_date>` text into a `LocalDate`. The XML's raw date string has
 *     already been format-validated by [[repcheck.ingestion.votes.xml.SenateVoteXmlDecoder]]; re-parsing here is
 *     cheap and keeps the DO self-contained. On unparseable dates, leaves `voteDate = None` rather than failing the
 *     whole vote — the detector's `updateDate` comparison does not depend on `voteDate`, and upstream analytics can
 *     handle `None`.
 *   - `VoteDO.updateDate` set to the parsed `voteDate` interpreted at 00:00 UTC, because senate.gov XML does not
 *     include a separate `updateDate` field. This makes the change detector's
 *     "incoming updateDate is newer than stored" comparison meaningful as soon as the DTO is re-ingested with a new
 *     `vote_date`.
 *   - `VoteDO.question` = XML's `<question>` text verbatim.
 *   - Positions: one `VotePositionDO` per senator in `dto.members`, with
 *     `memberId = None, lisMemberId = Some(resolvedLisId)` and cast/party/state enum-parsed from the XML strings. A
 *     senator whose natural key is missing from `lisMapping` is a defect in the LIS resolver (it should have upserted
 *     every senator seen on the DTO); this converter raises [[VoteConversionFailed]] in that case rather than silently
 *     dropping the position — position-list completeness is a scoring correctness invariant.
 */
private[pipeline] class SenateVoteConverter[F[_]: Async](logger: PipelineLogger[F]) {

  import SenateVoteConverter._

  /**
   * Convert one senate vote DTO + its LIS-resolution map into a (VoteDO, List[VotePositionDO]) pair.
   *
   * @param dto
   *   the XML-decoded roll call.
   * @param lisMap
   *   LIS natural key (e.g. `"S428"`) → `lis_members.id` Long. Must contain an entry for every senator in `dto.members`;
   *   call [[repcheck.ingestion.votes.lis.LisResolver.resolve]] on `dto` before invoking this converter.
   */
  def convert(
    dto: SenateVoteXmlDTO,
    lisMap: Map[String, Long],
    logCtx: LogContext,
  ): F[(VoteDO, List[VotePositionDO])] = {
    val naturalKey = VoteConversions.buildVoteNaturalKey(
      congress = dto.congress,
      chamber = "Senate",
      session = dto.session,
      rollCallNumber = dto.voteNumber,
    )

    val voteDo = buildVoteDO(dto, naturalKey)

    buildPositions(dto, lisMap, naturalKey, logCtx).map { positions =>
      (voteDo, positions)
    }
  }

  private def buildVoteDO(dto: SenateVoteXmlDTO, naturalKey: String): VoteDO = {
    val parsedDate = parseVoteDate(dto.voteDate)
    VoteDO(
      voteId = 0L,
      naturalKey = naturalKey,
      congress = dto.congress,
      chamber = Chamber.Senate,
      rollNumber = dto.voteNumber,
      sessionNumber = Some(dto.session),
      billId = None,
      question = Some(dto.question),
      voteType = Some(VoteType.fromQuestion(dto.question)),
      voteMethod = None,
      result = Some(dto.result),
      voteDate = parsedDate,
      legislationNumber = None,
      legislationType = None,
      legislationUrl = None,
      sourceDataUrl = None,
      updateDate = parsedDate.map(_.atStartOfDay().toInstant(ZoneOffset.UTC)),
      createdAt = None,
      updatedAt = None,
    )
  }

  private def buildPositions(
    dto: SenateVoteXmlDTO,
    lisMap: Map[String, Long],
    naturalKey: String,
    logCtx: LogContext,
  ): F[List[VotePositionDO]] =
    dto.members.traverse { member =>
      lisMap.get(member.lisMemberId) match {
        case Some(lisId) =>
          val cast  = VoteCast.fromString(member.voteCast).toOption
          val party = Party.fromString(member.party).toOption
          val state = UsState.fromString(member.state).toOption
          Async[F].pure(
            VotePositionDO(
              id = 0L,
              voteId = 0L,
              memberId = None,
              position = cast,
              partyAtVote = party,
              stateAtVote = state,
              createdAt = None,
              lisMemberId = Some(lisId),
            )
          )
        case None =>
          val err = VoteConversionFailed(
            naturalKey,
            s"LIS resolver did not produce a mapping for senator ${member.lisMemberId} — position list is incomplete",
          )
          logger.error(logCtx, err.getMessage, Some(err)) *>
            Async[F].raiseError[VotePositionDO](err)
      }
    }

}

private object SenateVoteConverter {

  private val LongWithDayOfWeek: DateTimeFormatter =
    DateTimeFormatter.ofPattern("EEEE, MMMM d, yyyy, hh:mm a", Locale.US)

  private val LongNoDayOfWeek: DateTimeFormatter =
    DateTimeFormatter.ofPattern("MMMM d, yyyy, hh:mm a", Locale.US)

  private val IsoLocalFormatter: DateTimeFormatter =
    DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss")

  /**
   * Parse the senate.gov `<vote_date>` string into a `LocalDate`. Tries the same formats the XML decoder validates
   * against; on failure, returns `None` so the conversion proceeds without a persisted date. The decoder has already
   * filtered out un-parseable dates upstream, so in the common case this function always returns `Some(date)`.
   */
  private[pipeline] def parseVoteDate(raw: String): Option[LocalDate] = {
    val normalized = raw.trim.replaceAll("\\s+", " ")
    val attempts: List[() => LocalDate] = List(
      () => java.time.OffsetDateTime.parse(raw).toLocalDate,
      () => java.time.LocalDateTime.parse(raw, IsoLocalFormatter).toLocalDate,
      () => java.time.LocalDateTime.parse(normalized, LongWithDayOfWeek).toLocalDate,
      () => java.time.LocalDateTime.parse(normalized, LongNoDayOfWeek).toLocalDate,
    )
    attempts.iterator
      .map(a => Try(a()).toOption)
      .find(_.isDefined)
      .flatten
  }

}
