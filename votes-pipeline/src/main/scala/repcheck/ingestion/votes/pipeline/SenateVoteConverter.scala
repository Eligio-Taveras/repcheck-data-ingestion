package repcheck.ingestion.votes.pipeline

import java.time.format.DateTimeFormatter
import java.time.{LocalDate, ZoneOffset}
import java.util.Locale

import scala.util.Try

import cats.effect.Async
import cats.syntax.all._

import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}
import repcheck.ingestion.votes.xml.SenateVoteUrls
import repcheck.shared.models.congress.common.{BillType, Chamber, Party, UsState}
import repcheck.shared.models.congress.dos.results.{UnresolvedVotePosition, VoteConversionResult}
import repcheck.shared.models.congress.dos.vote.VoteDO
import repcheck.shared.models.congress.dto.conversions.{BillConversions, VoteConversions}
import repcheck.shared.models.congress.dto.vote.{SenateVoteDocumentDTO, SenateVoteXmlDTO}
import repcheck.shared.models.congress.vote.{VoteCast, VoteType}

/**
 * Converts a senate.gov `<roll_call_vote>` XML document (as a decoded [[SenateVoteXmlDTO]]) into a
 * [[VoteConversionResult]] — `VoteDO` + `billNaturalKey` + `List[UnresolvedVotePosition]`.
 *
 * ==Document classification + bill resolution==
 *
 * Every senate.gov vote XML carries a `<document>` element (required per senate.gov's schema) identifying the
 * underlying bill, resolution, nomination, or treaty. The converter classifies `document.documentType`:
 *
 *   - Bill-like (`"S."`, `"H.R."`, `"S.J.Res."`, `"H.J.Res."`, `"S.Res."`, `"H.Res."`, `"S.Con.Res."`, `"H.Con.Res."`):
 *     normalizes the type to a [[BillType]] enum value, constructs a bill natural key via
 *     [[BillConversions.buildBillNaturalKey]], calls `billLookup` to resolve the bill's surrogate id (the same
 *     placeholder+lookup flow used by the House converter), and populates `VoteDO.billId = Some(resolvedId)`,
 *     `legislationType = Some(billType)`, `legislationNumber = Some(documentNumber)`, and `legislationUrl` via
 *     [[SenateVoteConverter.buildCongressGovBillUrl]].
 *   - `"PN"` (Presidential Nomination) or `"Treaty Doc."`: `billId = None`, `legislationType = None`, `legislationUrl
 *     \= None`. Logged at info; nominations and treaties are legitimate votes that don't link to our `bills` table.
 *   - Any other documentType: `billId = None` and friends, logged at warn so operators can grow the type map if a new
 *     senate.gov value appears.
 *
 * ==URL derivation==
 *
 * Senate XML does not carry `legislationUrl` or `sourceDataUrl` fields, but both are derivable from known inputs:
 *   - `sourceDataUrl` — the senate.gov URL the vote XML was fetched from. Derived via [[SenateVoteUrls.voteXmlUrl]] so
 *     the client and the DO always agree. Always populated.
 *   - `legislationUrl` — the congress.gov bill page URL, e.g. `https://www.congress.gov/bill/119/senate-bill/1071`.
 *     Derived via [[SenateVoteConverter.buildCongressGovBillUrl]] for bill-like classifications only; `None` otherwise.
 *     Pattern verified against real Congress.gov API responses (`api.congress.gov/v3/house-vote/119/1/17` returns
 *     exactly this shape).
 *
 * ==Output shape==
 *   - `VoteDO.chamber = Chamber.Senate`.
 *   - `VoteDO.billId` — real resolved id (bill-like) or `None` (non-bill).
 *   - `VoteDO.voteType` classified from the `<question>` via [[VoteType.fromQuestion]] — same rule House uses.
 *   - `VoteDO.voteDate` parsed from the XML's `<vote_date>` text into a `LocalDate`.
 *   - `VoteDO.updateDate` set to the parsed `voteDate` at 00:00 UTC (senate.gov XML has no separate updateDate).
 *   - Positions: one [[UnresolvedVotePosition]] per senator, with `memberSource = Right(lisMemberId)`. The processor
 *     resolves each LIS id to a `lis_members.id` via [[repcheck.ingestion.votes.lis.LisResolver]] and materializes the
 *     Senate-arm `VotePositionDO` rows inside the persister's transaction.
 *
 * @param senateBaseUrl
 *   senate.gov base URL (e.g. `https://www.senate.gov/legislative/LIS`) — used to derive `sourceDataUrl`. In tests we
 *   pass a WireMock URL; in production we pass the config default. Must agree with the URL the
 *   [[repcheck.ingestion.votes.xml.SenateVoteXmlClient]] fetched from.
 */
class SenateVoteConverter[F[_]: Async](
  logger: PipelineLogger[F],
  senateBaseUrl: String,
) {

  import SenateVoteConverter._

  /**
   * Convert one senate vote DTO into a [[VoteConversionResult]]. `billLookup` is invoked once when the document
   * classifies as bill-like; non-bill documents (PN, Treaty, unknown) leave `billId = None`.
   */
  def convert(
    dto: SenateVoteXmlDTO,
    billLookup: String => F[Option[Long]],
    logCtx: LogContext,
  ): F[VoteConversionResult] = {
    val naturalKey = VoteConversions.buildVoteNaturalKey(
      congress = dto.congress,
      chamber = "Senate",
      session = dto.session,
      rollCallNumber = dto.voteNumber,
    )

    for {
      classification <- classifyDocument(dto.document, naturalKey, logCtx)
      resolvedBillId <- classification.billNaturalKey match {
        case Some(nk) => billLookup(nk)
        case None     => Async[F].pure(Option.empty[Long])
      }
      voteDo    = buildVoteDO(dto, naturalKey, classification, resolvedBillId)
      positions = buildUnresolvedPositions(dto)
    } yield VoteConversionResult(
      vote = voteDo,
      billNaturalKey = classification.billNaturalKey,
      positions = positions,
    )
  }

  /**
   * Classify the `<document>` element, log at the appropriate level, and return a [[DocumentClassification]] with the
   * derived bill natural key (for bill-like types) and the normalized [[BillType]] / `legislationNumber`.
   *
   * ==Why the documentCongress > 0 gate==
   *
   * `SenateVoteXmlDecoder.decodeDocument` is intentionally tolerant of older votes (109th Congress era and earlier)
   * where senate.gov emits a `<document>` block with bill_type/bill_number populated but a self-closing or empty
   * `<document_congress/>` — the decoder defaults `documentCongress = 0` rather than failing the whole vote. The
   * decoder's intent is that those votes route through the non-bill branch (no bill linkage). Without this gate the
   * classifier would happily build a bill natural key like `"0-S-1059"` and `BillRepository.upsertPlaceholder` would
   * create an orphan bill row with `congress=0` that no future write can heal (the `(congress, bill_type, number)`
   * UNIQUE constraint means a later real `(118, S, 1059)` upsert lands as a separate row, not an UPDATE of the
   * placeholder). Surfaced empirically on the local stack — 909 such orphans accumulated before this gate.
   */
  private[pipeline] def classifyDocument(
    document: SenateVoteDocumentDTO,
    voteNaturalKey: String,
    logCtx: LogContext,
  ): F[DocumentClassification] =
    normalizeDocumentType(document.documentType) match {
      case Right(billType) if document.documentCongress > 0 =>
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
            legislationUrl = buildCongressGovBillUrl(document.documentCongress, billType, document.documentNumber),
          )
        )
      case Right(billType) =>
        // documentType parsed to a real BillType but documentCongress is missing/zero — old-vote XML shape
        // where <document_congress/> is empty. Persist without bill linkage rather than creating an orphan
        // congress=0 placeholder. See scaladoc above for the full rationale.
        logger
          .info(
            logCtx,
            s"Senate vote $voteNaturalKey has bill documentType '${billType.apiValue}' but missing or zero " +
              s"documentCongress (${document.documentCongress}) — persisting with billId=None",
          )
          .as(DocumentClassification.empty)
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
    resolvedBillId: Option[Long],
  ): VoteDO = {
    val parsedDate    = parseVoteDate(dto.voteDate)
    val sourceDataUrl = SenateVoteUrls.voteXmlUrl(senateBaseUrl, dto.congress, dto.session, dto.voteNumber)
    VoteDO(
      voteId = 0L,
      naturalKey = naturalKey,
      congress = dto.congress,
      chamber = Chamber.Senate,
      rollNumber = dto.voteNumber,
      sessionNumber = Some(dto.session),
      billId = resolvedBillId,
      question = Some(dto.question),
      voteType = Some(VoteType.fromQuestion(dto.question)),
      voteMethod = None,
      result = Some(dto.result),
      voteDate = parsedDate,
      legislationNumber = classification.legislationNumber,
      legislationType = classification.legislationType,
      legislationUrl = classification.legislationUrl,
      sourceDataUrl = Some(sourceDataUrl),
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
   * `billNaturalKey` + `legislationType` + `legislationNumber` + `legislationUrl` are all populated. Non-bill documents
   * (PN, Treaty Doc., unknown) produce the empty classification.
   */
  final case class DocumentClassification(
    billNaturalKey: Option[String],
    legislationType: Option[BillType],
    legislationNumber: Option[String],
    legislationUrl: Option[String],
  )

  object DocumentClassification {
    val empty: DocumentClassification = DocumentClassification(None, None, None, None)
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

  /**
   * Build the canonical congress.gov bill page URL for a bill-like classification. Returns `Some(url)` only for
   * BillType variants that correspond to routable congress.gov `/bill/{congress}/{slug}/{number}` pages — the eight
   * "bill-like" variants that [[normalizeDocumentType]] can actually produce. Returns `None` for the other BillType
   * variants (PL, STAT, USC, SRPT, HRPT) which are legislative artifacts without a bill-page URL; in practice
   * `classifyDocument` never feeds these through because `normalizeDocumentType` restricts the input set.
   *
   * Pattern verified against the live Congress.gov API (`api.congress.gov/v3/house-vote/119/1/17` returned
   * `"legislationUrl":"https://www.congress.gov/bill/119/house-bill/30"` — bare `congress`, no `th-congress` suffix).
   */
  private[pipeline] def buildCongressGovBillUrl(congress: Int, billType: BillType, number: String): Option[String] =
    billTypeUrlSlug(billType).map(slug => s"https://www.congress.gov/bill/${congress.toString}/$slug/$number")

  /**
   * Slug table for congress.gov bill URLs. Verified via live Congress.gov responses for HR → `house-bill` (vote
   * 119/1/17) and HCONRES → `house-concurrent-resolution` (vote 119/1/100). Non-bill BillType variants (PL, STAT, USC,
   * SRPT, HRPT) fall through to `None` — they aren't bills and have no such URL.
   */
  private[pipeline] def billTypeUrlSlug(billType: BillType): Option[String] = billType match {
    case BillType.S       => Some("senate-bill")
    case BillType.HR      => Some("house-bill")
    case BillType.SJRES   => Some("senate-joint-resolution")
    case BillType.HJRES   => Some("house-joint-resolution")
    case BillType.SRES    => Some("senate-resolution")
    case BillType.HRES    => Some("house-resolution")
    case BillType.SCONRES => Some("senate-concurrent-resolution")
    case BillType.HCONRES => Some("house-concurrent-resolution")
    case BillType.PL | BillType.STAT | BillType.USC | BillType.SRPT | BillType.HRPT => None
  }

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
