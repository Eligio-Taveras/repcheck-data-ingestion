package repcheck.ingestion.votes.pipeline

import java.time.format.DateTimeFormatter
import java.time.{LocalDate, ZoneOffset}
import java.util.Locale

import scala.util.Try
import scala.util.matching.Regex

import cats.effect.Async
import cats.syntax.all._

import doobie.implicits._
import doobie.util.transactor.Transactor

import repcheck.ingestion.amendments.persistence.AmendmentRepository
import repcheck.ingestion.bills.common.persistence.BillRepository
import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}
import repcheck.ingestion.votes.metrics.AmendmentPlaceholderSkipCounter
import repcheck.ingestion.votes.xml.{SenateVoteAmendmentFields, SenateVoteEnvelope, SenateVoteUrls}
import repcheck.pipeline.models.constants.Constants
import repcheck.shared.models.congress.amendment.{AmendmentType, LegislationRef}
import repcheck.shared.models.congress.common.{BillType, Chamber, Party, UsState}
import repcheck.shared.models.congress.dos.results.{UnresolvedVotePosition, VoteConversionResult}
import repcheck.shared.models.congress.dos.vote.VoteDO
import repcheck.shared.models.congress.dto.conversions.{BillConversions, VoteConversions}
import repcheck.shared.models.congress.dto.vote.{SenateVoteDocumentDTO, SenateVoteXmlDTO}
import repcheck.shared.models.congress.vote.{VoteCast, VoteType}

/**
 * Converts a senate.gov `<roll_call_vote>` XML envelope (as a [[SenateVoteEnvelope]]) into a [[VoteConversionResult]] —
 * `VoteDO` + `billNaturalKey` + `List[UnresolvedVotePosition]`.
 *
 * ==Document classification + bill resolution==
 *
 * Every senate.gov vote XML carries a `<document>` element (required per senate.gov's schema) identifying the
 * underlying bill, resolution, nomination, treaty, OR amendment. The converter classifies `document.documentType` into
 * a [[LegislationRef]]:
 *
 *   - [[LegislationRef.Bill]] (bill-like types `"S."`, `"H.R."`, `"S.J.Res."`, etc.): builds the bill natural key,
 *     calls `billLookup` to upsert + resolve the surrogate id, populates `VoteDO.billId = Some(resolvedId)` and
 *     `legislation_*` columns from [[BillType]].
 *   - [[LegislationRef.Amendment]] (`"S.Amdt."`, `"H.Amdt."`): parses the amendment number from the top-level
 *     `<amendment_number>` element (which carries the type prefix + number), builds the amendment natural key
 *     `{congress}-{TYPE}-{number}`, and calls [[AmendmentRepository.upsertPlaceholder]] to reserve a row that the
 *     amendments-pipeline will hydrate later. `VoteDO.billId = None` (votes don't FK to amendments per §7.4 OQ2),
 *     `legislation_type = None` (shared-models `VoteDO.legislationType: Option[BillType]` cannot hold an amendment type
 *     until shared-models bumps), `legislation_number = Some(number)`. If `<amendment_to_document_number>` is
 *     populated, the converter ALSO calls `billRepo.upsertPlaceholder` for the parent bill's natural key — that gives
 *     the amendments-pipeline somewhere to attach `bill_id` when it later hydrates the amendment.
 *   - `"PN"` / `"Treaty Doc."`: non-bill, non-amendment. Persisted with `billId = None` + INFO log.
 *   - Any other documentType: `billId = None`, WARN log.
 *
 * ==Pre-102 amendment skip==
 *
 * Per Q9 of the §7 plan, amendments-pipeline is scoped to congresses ≥ 102 ([[Constants.MinAmendmentCongress]]). When a
 * Senate vote in an older congress is amendment-classified, the converter SKIPS the placeholder upsert (incrementing
 * the `pre_102_amendment` skip counter) but still persists the vote row with `legislation_number` set — the vote itself
 * is recorded, just without an amendment-placeholder side-effect.
 *
 * ==URL derivation==
 *
 * Senate XML does not carry `legislationUrl` or `sourceDataUrl` fields, but both are derivable from known inputs:
 *   - `sourceDataUrl` — derived via [[SenateVoteUrls.voteXmlUrl]]; always populated.
 *   - `legislationUrl` — for bill-like classifications only via [[SenateVoteConverter.buildCongressGovBillUrl]]; `None`
 *     for amendments (no published Library-of-Congress slug for amendment URLs as of 2026-05; deferred per §7.4 area).
 *
 * @param senateBaseUrl
 *   senate.gov base URL — used to derive `sourceDataUrl`. Must agree with the URL [[SenateVoteXmlClient]] fetched from.
 * @param amendmentRepo
 *   The amendments-pipeline's repository, used by the amendment branch to reserve a placeholder row before persisting
 *   the vote.
 * @param billRepo
 *   The bills-common repository, used by the amendment branch to seed a placeholder for the parent bill referenced by
 *   `<amendment_to_document_number>` (so the amendments-pipeline's later hydration finds a row to attach `bill_id` to).
 * @param skipCounter
 *   Increments `pre_102_amendment` when an amendment-typed vote in a pre-102 congress is encountered.
 */
class SenateVoteConverter[F[_]: Async](
  logger: PipelineLogger[F],
  senateBaseUrl: String,
  amendmentRepo: AmendmentRepository[doobie.ConnectionIO],
  billRepo: BillRepository[doobie.ConnectionIO],
  xa: Transactor[F],
  skipCounter: AmendmentPlaceholderSkipCounter,
) {

  import SenateVoteConverter._

  /**
   * Convert one senate vote envelope into a [[VoteConversionResult]]. `billLookup` is invoked once when the document
   * classifies as bill-like; non-bill documents (PN, Treaty, unknown) leave `billId = None`. Amendment-classified
   * documents call [[AmendmentRepository.upsertPlaceholder]] (and [[BillRepository.upsertPlaceholder]] for the parent
   * bill ref) and produce `billId = None` per §7.4.
   */
  def convert(
    envelope: SenateVoteEnvelope,
    billLookup: String => F[Option[Long]],
    logCtx: LogContext,
  ): F[VoteConversionResult] = {
    val dto = envelope.dto
    val naturalKey = VoteConversions.buildVoteNaturalKey(
      congress = dto.congress,
      chamber = "Senate",
      session = dto.session,
      rollCallNumber = dto.voteNumber,
    )

    for {
      classification <- classifyDocument(dto.document, envelope.amendmentFields, dto.congress, naturalKey, logCtx)
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
   * Classify the `<document>` element + amendment-vote fields, log at the appropriate level, perform the amendment-side
   * placeholder upserts when the vote is amendment-classified, and return a [[DocumentClassification]] with the derived
   * bill natural key (for bill-like types) and the parsed amendment number (for amendment types).
   *
   * ==Why the documentCongress > 0 gate==
   *
   * `SenateVoteXmlDecoder.decodeDocument` is intentionally tolerant of older votes (109th Congress era and earlier)
   * where senate.gov emits a `<document>` block with bill_type/bill_number populated but a self-closing or empty
   * `<document_congress/>` — the decoder defaults `documentCongress = 0` rather than failing the whole vote. The
   * decoder's intent is that those votes route through the non-bill branch (no bill linkage). Without this gate the
   * classifier would happily build a bill natural key like `"0-S-1059"` and `BillRepository.upsertPlaceholder` would
   * create an orphan bill row with `congress=0` that no future write can heal. 909 such orphans surfaced empirically
   * before this gate.
   */
  private[pipeline] def classifyDocument(
    document: SenateVoteDocumentDTO,
    amendmentFields: SenateVoteAmendmentFields,
    voteCongress: Int,
    voteNaturalKey: String,
    logCtx: LogContext,
  ): F[DocumentClassification] =
    normalizeDocumentType(document.documentType) match {
      case Right(LegislationRef.Bill(billType)) if document.documentCongress > 0 =>
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
      case Right(LegislationRef.Bill(billType)) =>
        // documentType parsed to a real BillType but documentCongress is missing/zero — old-vote XML shape.
        // Persist without bill linkage rather than creating an orphan congress=0 placeholder.
        logger
          .info(
            logCtx,
            s"Senate vote $voteNaturalKey has bill documentType '${billType.apiValue}' but missing or zero " +
              s"documentCongress (${document.documentCongress.toString}) — persisting with billId=None",
          )
          .as(DocumentClassification.empty)
      case Right(LegislationRef.Amendment(amendmentType)) =>
        handleAmendment(amendmentType, amendmentFields, voteCongress, voteNaturalKey, logCtx)
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

  private def handleAmendment(
    amendmentType: AmendmentType,
    amendmentFields: SenateVoteAmendmentFields,
    voteCongress: Int,
    voteNaturalKey: String,
    logCtx: LogContext,
  ): F[DocumentClassification] =
    amendmentFields.amendmentNumber.flatMap(parseAmendmentNumber) match {
      case None =>
        // Empty/missing/malformed <amendment_number> — persist without amendment linkage. Mirrors the empty-
        // documentCongress fallback for bill-typed votes: the vote itself still records, but no placeholder
        // is created (per §7.4 area file: "fall back to skipping placeholder creation but still persisting").
        logger
          .info(
            logCtx,
            s"Senate vote $voteNaturalKey has amendment documentType '${amendmentType.toString}' but " +
              s"<amendment_number> '${amendmentFields.amendmentNumber.getOrElse("")}' is missing or unparseable — " +
              "persisting with no amendment placeholder",
          )
          .as(DocumentClassification.empty)
      case Some(amendmentNumber) =>
        val amendmentNK = s"${voteCongress.toString}-${amendmentType.toString}-$amendmentNumber"
        val placeholderEffect: F[Unit] =
          if (voteCongress < Constants.MinAmendmentCongress) {
            // Per Q9: amendments-pipeline is scoped to congress >= 102. Older amendment-typed votes still
            // record the vote row (with legislation_number set) but skip the amendment-side write so we
            // don't pollute the `amendments` table with rows the owner pipeline will never visit.
            skipCounter.incPre102Skip[F] *>
              logger.info(
                logCtx,
                s"Skipping pre-102 amendment placeholder for $amendmentNK (vote $voteNaturalKey)",
              )
          } else {
            amendmentRepo.upsertPlaceholder(amendmentNK).transact(xa).handleErrorWith { err =>
              logger.error(
                logCtx,
                s"Amendment placeholder failed for $amendmentNK (vote $voteNaturalKey)",
                Some(err),
              ) *> Async[F].raiseError[Unit](err)
            }
          }
        val parentBillEffect: F[Unit] =
          amendmentFields.amendmentToDocumentNumber.flatMap(parseAmendedBillRef(_, voteCongress)) match {
            case Some(billNK) if voteCongress >= Constants.MinAmendmentCongress =>
              // Reserve the parent bill so the amendments-pipeline's later hydration can resolve bill_id.
              // Errors at this step are logged + raised — same severity as the amendment placeholder itself.
              billRepo.upsertPlaceholder(billNK).transact(xa).handleErrorWith { err =>
                logger.error(
                  logCtx,
                  s"Parent-bill placeholder failed for $billNK (amendment $amendmentNK, vote $voteNaturalKey)",
                  Some(err),
                ) *> Async[F].raiseError[Unit](err)
              }
            case _ => Async[F].unit
          }
        for {
          _ <- placeholderEffect
          _ <- parentBillEffect
        } yield DocumentClassification(
          // billNaturalKey is the BILL key (used by the caller to invoke `billLookup` for surrogate-id resolution).
          // Amendments don't have a bill_id, so amendmentNK is NOT the billNaturalKey here.
          billNaturalKey = None,
          // shared-models VoteDO.legislationType only accepts BillType — until shared-models bumps the type to a
          // sealed bill-or-amendment kind, leave this None for amendment votes. legislationNumber still records.
          legislationType = None,
          legislationNumber = Some(amendmentNumber),
          legislationUrl = None,
        )
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
   * `billNaturalKey` + `legislationType` + `legislationNumber` + `legislationUrl` are all populated. Non-bill / non-
   * amendment documents (PN, Treaty Doc., unknown) produce the empty classification. Amendment classifications populate
   * `legislationNumber` only — see SenateVoteConverter scaladoc for the rationale.
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
   * Senate.gov's `<document_type>` values are period-punctuated ("S.", "H.R.", "S.J.Res.", "S.Amdt.", "H.Amdt.", etc.).
   * Normalize to a [[LegislationRef]] (bill or amendment) when possible; return a tagged rejection otherwise so the
   * caller can choose between info-level (recognized non-bill) and warn-level (unknown) logging.
   *
   * `"S.U.Amdt."` is intentionally OMITTED — Senate Unanimous Amendments only existed in the 97th-98th Congresses
   * (1981-1985), well before our 102nd-Congress floor (per Q9 of planning). We will never see it in modern Senate vote
   * XML, so adding the case would be dead code.
   */
  private[pipeline] def normalizeDocumentType(raw: String): Either[NonBillOrUnknown, LegislationRef] = {
    val cleaned = raw.trim
    cleaned match {
      case "S."          => Right(LegislationRef.Bill(BillType.S))
      case "H.R."        => Right(LegislationRef.Bill(BillType.HR))
      case "S.J.Res."    => Right(LegislationRef.Bill(BillType.SJRES))
      case "H.J.Res."    => Right(LegislationRef.Bill(BillType.HJRES))
      case "S.Res."      => Right(LegislationRef.Bill(BillType.SRES))
      case "H.Res."      => Right(LegislationRef.Bill(BillType.HRES))
      case "S.Con.Res."  => Right(LegislationRef.Bill(BillType.SCONRES))
      case "H.Con.Res."  => Right(LegislationRef.Bill(BillType.HCONRES))
      case "S.Amdt."     => Right(LegislationRef.Amendment(AmendmentType.SAMDT))
      case "H.Amdt."     => Right(LegislationRef.Amendment(AmendmentType.HAMDT))
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

  /**
   * Pattern for `<amendment_number>` — `"S.Amdt. 2137"` → captures `"2137"`. Per L5, the `(?U)` flag makes `\s` match
   * Unicode whitespace including the non-breaking space (U+00A0) that senate.gov occasionally emits between the prefix
   * and the digits. Without `(?U)`, NBSP-containing documents would silently fail to parse.
   *
   * Accepts `S.U.Amdt.` (Senate Unanimous) for tolerance — the converter's `normalizeDocumentType` filters this case
   * upstream so we never actually see it, but the regex stays inclusive in case Senate XML emits `S.U.Amdt.` text
   * inside `<amendment_number>` for an oddly-classified vote.
   */
  private[pipeline] val AmendmentNumberPattern: Regex =
    """(?U)^(?:S\.U\.Amdt\.|S\.Amdt\.|H\.Amdt\.)\s+(\d+)$""".r

  /**
   * Parse `"S.Amdt. 2137"` → `Some("2137")`. Returns `None` for any input that doesn't match the strict prefix +
   * whitespace + digits shape — including malformed prefixes (`"X.Amdt. 100"`), missing whitespace, non-numeric tails,
   * or a fully empty string.
   */
  private[pipeline] def parseAmendmentNumber(raw: String): Option[String] =
    raw.trim match {
      case AmendmentNumberPattern(num) => Some(num)
      case _                           => None
    }

  /**
   * Parse `<amendment_to_document_number>` text (e.g. `"H.R. 3684"`) into the parent bill's full natural key
   * (`"117-HR-3684"`). The text doesn't carry the congress so we use the AMENDMENT's congress, which the caller threads
   * through. Returns `None` for unrecognized prefix patterns or non-numeric numbers — defensive; the senate.gov
   * upstream uses the same canonical strings as `normalizeDocumentType`.
   *
   * Splits on the LAST whitespace run so we tolerate the same NBSP/multi-space variations that `<amendment_number>` can
   * contain (`"H.R. 3684"`, `"H.R. 3684"`, etc.).
   */
  private[pipeline] def parseAmendedBillRef(raw: String, congress: Int): Option[String] = {
    val cleaned = raw.trim
    // Split off the last whitespace-separated token as the number; the rest is the prefix. Java's `\s` is
    // ASCII-only by default; passing `(?U)` would cover NBSP. Use a regex split to keep all variants in one path.
    val parts = cleaned.split("(?U)\\s+")
    if (parts.length < 2) None
    else {
      val number = parts(parts.length - 1)
      val prefix = parts.dropRight(1).mkString(" ")
      if (number.isEmpty || !number.forall(_.isDigit)) None
      else
        normalizeDocumentType(prefix) match {
          case Right(LegislationRef.Bill(billType)) =>
            Some(BillConversions.buildBillNaturalKey(congress, billType.apiValue, number))
          case _ => None
        }
    }
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
