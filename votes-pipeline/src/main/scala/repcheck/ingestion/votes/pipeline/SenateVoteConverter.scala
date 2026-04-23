package repcheck.ingestion.votes.pipeline

import java.time.format.DateTimeFormatter
import java.time.{LocalDate, ZoneOffset}
import java.util.Locale

import scala.util.Try

import cats.effect.Async
import cats.syntax.all._

import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}
import repcheck.shared.models.congress.common.{BillType, Chamber, Party, UsState}
import repcheck.shared.models.congress.dos.results.{UnresolvedVotePosition, VoteConversionResult}
import repcheck.shared.models.congress.dos.vote.VoteDO
import repcheck.shared.models.congress.dto.conversions.{BillConversions, VoteConversions}
import repcheck.shared.models.congress.dto.vote.{SenateVoteDocumentDTO, SenateVoteXmlDTO}
import repcheck.shared.models.congress.vote.{VoteCast, VoteType}

/**
 * Converts a senate.gov `<roll_call_vote>` XML document (as a decoded [[SenateVoteXmlDTO]]) into a
 * [[VoteConversionResult]] — `VoteDO` + `billNaturalKey` + `List[UnresolvedVotePosition]`. Like [[HouseVoteConverter]],
 * this class does NOT resolve any ids to database rows; the processor handles member and bill resolution after the
 * converter produces its pure structural output.
 *
 * ==Document classification==
 *
 * Every senate.gov vote XML carries a `<document>` element (required per senate.gov's schema) identifying the
 * underlying bill, resolution, nomination, or treaty. The converter classifies `document.documentType`:
 *
 *   - Bill-like (`"S."`, `"H.R."`, `"S.J.Res."`, `"H.J.Res."`, `"S.Res."`, `"H.Res."`, `"S.Con.Res."`, `"H.Con.Res."`):
 *     normalizes the type to a [[BillType]] enum value (apiValue), constructs a bill natural key via
 *     [[BillConversions.buildBillNaturalKey]], and populates `VoteDO.legislationType` / `legislationNumber` /
 *     `legislationUrl`. The processor resolves this natural key to a `bills.id` via the same placeholder+lookup flow
 *     used for House votes.
 *   - `"PN"` (Presidential Nomination), `"Treaty Doc."`, or any other unknown documentType: `billNaturalKey = None`,
 *     `VoteDO.billId = None`, `legislationType = None`, etc. Logged at info (for PN / Treaty) or warn (for unknown).
 *     Nominations and treaties are legitimate votes; they just don't link to our `bills` table.
 *
 * ==Output shape==
 *   - `VoteDO.chamber = Chamber.Senate`.
 *   - `VoteDO.billId = None` (processor overwrites after resolving `billNaturalKey`).
 *   - `VoteDO.voteType` classified from the `<question>` via [[VoteType.fromQuestion]] — same rule House uses.
 *   - `VoteDO.voteDate` parsed from the XML's `<vote_date>` text into a `LocalDate`.
 *   - `VoteDO.updateDate` set to the parsed `voteDate` at 00:00 UTC (senate.gov XML has no separate updateDate).
 *   - Positions: one [[UnresolvedVotePosition]] per senator, with `memberSource = Right(lisMemberId)`. The processor
 *     resolves each LIS id to a `lis_members.id` via [[repcheck.ingestion.votes.lis.LisResolver]] and materializes the
 *     Senate-arm `VotePositionDO` rows inside the persister's transaction.
 */
private[pipeline] class SenateVoteConverter[F[_]: Async](logger: PipelineLogger[F]) {

  import SenateVoteConverter._

  /**
   * Convert one senate vote DTO into a [[VoteConversionResult]].
   */
  def convert(dto: SenateVoteXmlDTO, logCtx: LogContext): F[VoteConversionResult] = {
    val naturalKey = VoteConversions.buildVoteNaturalKey(
      congress = dto.congress,
      chamber = "Senate",
      session = dto.session,
      rollCallNumber = dto.voteNumber,
    )

    for {
      docClassification <- classifyDocument(dto.document, naturalKey, logCtx)
      voteDo    = buildVoteDO(dto, naturalKey, docClassification)
      positions = buildUnresolvedPositions(dto)
    } yield VoteConversionResult(
      vote = voteDo,
      billNaturalKey = docClassification.billNaturalKey,
      positions = positions,
    )
  }

  /**
   * Classify the `<document>` element, log at the appropriate level, and return:
   *   - `billNaturalKey: Option[String]` — populated for bill-like types (drives `VoteDO.billId` resolution upstream).
   *   - `legislationType: Option[BillType]` — populated for bill-like types.
   *   - `legislationNumber: Option[String]` — populated for bill-like types (raw number from senate.gov).
   */
  private[pipeline] def classifyDocument(
    document: SenateVoteDocumentDTO,
    voteNaturalKey: String,
    logCtx: LogContext,
  ): F[DocumentClassification] =
    normalizeDocumentType(document.documentType) match {
      case Right(billType) =>
        val billNK = BillConversions.buildBillNaturalKey(
          congress = document.documentCongress,
          billType = billType.apiValue,
          number = document.documentNumber,
        )
        Async[F].pure(
          DocumentClassification(
            billNaturalKey = Some(billNK),
            legislationType = Some(billType),
            legislationNumber = Some(document.documentNumber),
          )
        )
      case Left(NonBillDocument(docType)) =>
        logger
          .info(
            logCtx,
            s"Senate vote $voteNaturalKey has non-bill documentType '$docType' (${document.documentName}) — " +
              "persisting with billId=None",
          )
          .as(DocumentClassification.empty)
      case Left(UnknownDocument(docType)) =>
        logger
          .warn(
            logCtx,
            s"Senate vote $voteNaturalKey has unrecognized documentType '$docType' (${document.documentName}) — " +
              "persisting with billId=None; add the type to normalizeDocumentType if it should resolve to a bill",
          )
          .as(DocumentClassification.empty)
    }

  private def buildVoteDO(
    dto: SenateVoteXmlDTO,
    naturalKey: String,
    classification: DocumentClassification,
  ): VoteDO = {
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
      legislationNumber = classification.legislationNumber,
      legislationType = classification.legislationType,
      legislationUrl = None,
      sourceDataUrl = None,
      updateDate = parsedDate.map(_.atStartOfDay().toInstant(ZoneOffset.UTC)),
      createdAt = None,
      updatedAt = None,
    )
  }

  /**
   * Turn each senator row into an [[UnresolvedVotePosition]] with `memberSource = Right(lisMemberId)` (Senate arm per
   * the shared-models comment on `memberSource`). Vote cast, party, and state are enum-parsed defensively — unparseable
   * strings become `None` so the position still materializes.
   */
  private[pipeline] def buildUnresolvedPositions(dto: SenateVoteXmlDTO): List[UnresolvedVotePosition] =
    dto.members.map { m =>
      UnresolvedVotePosition(
        memberSource = Right(m.lisMemberId),
        voteCast = VoteCast.fromString(m.voteCast).toOption,
        partyAtVote = Party.fromString(m.party).toOption,
        stateAtVote = UsState.fromString(m.state).toOption,
      )
    }

}

private[pipeline] object SenateVoteConverter {

  /**
   * Outcome of [[SenateVoteConverter.classifyDocument]]. When the document represents a bill or resolution the
   * `billNaturalKey` is populated and the processor resolves it downstream; non-bill documents (PN, Treaty Doc.,
   * unknown) produce the empty classification.
   */
  final case class DocumentClassification(
    billNaturalKey: Option[String],
    legislationType: Option[BillType],
    legislationNumber: Option[String],
  )

  object DocumentClassification {
    val empty: DocumentClassification = DocumentClassification(None, None, None)
  }

  /**
   * Senate.gov's `<document_type>` values are period-punctuated ("S.", "H.R.", "S.J.Res.", etc.). Normalize to a
   * `BillType` enum when possible; return a tagged rejection otherwise so the caller can choose between info-level
   * (recognized non-bill) and warn-level (unknown) logging.
   */
  private[pipeline] def normalizeDocumentType(raw: String): Either[NonBillOrUnknown, BillType] = {
    val cleaned = raw.trim
    cleaned match {
      case "S."          => Right(BillType.S)
      case "H.R."        => Right(BillType.HR)
      case "S.J.Res."    => Right(BillType.SJRES)
      case "H.J.Res."    => Right(BillType.HJRES)
      case "S.Res."      => Right(BillType.SRES)
      case "H.Res."      => Right(BillType.HRES)
      case "S.Con.Res."  => Right(BillType.SCONRES)
      case "H.Con.Res."  => Right(BillType.HCONRES)
      case "PN"          => Left(NonBillDocument(cleaned))
      case "Treaty Doc." => Left(NonBillDocument(cleaned))
      case other         => Left(UnknownDocument(other))
    }
  }

  sealed trait NonBillOrUnknown {
    def rawType: String
  }

  final case class NonBillDocument(rawType: String) extends NonBillOrUnknown
  final case class UnknownDocument(rawType: String) extends NonBillOrUnknown

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
