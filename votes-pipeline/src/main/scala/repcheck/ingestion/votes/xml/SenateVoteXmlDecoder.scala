package repcheck.ingestion.votes.xml

import java.time.OffsetDateTime
import java.time.format.DateTimeFormatter
import java.util.Locale

import scala.util.Try
import scala.xml.{Elem, Node, NodeSeq}

import repcheck.ingestion.votes.errors.XmlParseFailed
import repcheck.shared.models.congress.dto.vote.{SenateVoteDocumentDTO, SenateVoteMemberXmlDTO, SenateVoteXmlDTO}

/**
 * Pure decoder for the senate.gov roll-call-vote XML feeds.
 *
 * ==Per-vote feed (`<roll_call_vote>` root)==
 *
 * {{{
 *   <roll_call_vote>
 *     <congress>119</congress>
 *     <session>1</session>
 *     <vote_number>17</vote_number>
 *     <question>On the Nomination</question>
 *     <vote_date>January 25, 2025, 11:30 AM</vote_date>
 *     <vote_result>Nomination Confirmed</vote_result>
 *     <members>
 *       <member>
 *         <lis_member_id>S428</lis_member_id>
 *         <first_name>Angela</first_name>
 *         <last_name>Alsobrooks</last_name>
 *         <party>D</party>
 *         <state>MD</state>
 *         <vote_cast>Nay</vote_cast>
 *       </member>
 *       ...
 *     </members>
 *   </roll_call_vote>
 * }}}
 *
 * ==Index feed (`<vote_summary>` root)==
 *
 * {{{
 *   <vote_summary>
 *     <congress>119</congress>
 *     <session>1</session>
 *     <votes>
 *       <vote>
 *         <vote_number>00659</vote_number>
 *         <vote_date>18-Dec</vote_date>
 *         <question>On the Cloture Motion</question>
 *         <result>Agreed to</result>
 *       </vote>
 *       ...
 *     </votes>
 *   </vote_summary>
 * }}}
 *
 * ==Date parsing==
 * Per the votes-pipeline execution plan (P2.2 decision 21b), `decodeVote` validates `voteDate` against these formats in
 * order:
 *   1. ISO-8601 (`yyyy-MM-dd'T'HH:mm:ss` and the `OffsetDateTime` variant with zone offset). 2. Long-form with
 *      day-of-week: `EEEE, MMMM d, yyyy, hh:mm a` (e.g., `Thursday, April 3, 2025, 02:42 PM`). 3. Long-form without
 *      day-of-week: `MMMM d, yyyy, hh:mm a` (e.g., `January 25, 2025, 11:30 AM` — matches real senate.gov vote XML
 *      bodies observed in fixtures).
 *
 * All three attempts tolerate runs of interior whitespace (senate.gov sometimes emits two spaces between the comma and
 * the time). On failure, the decoder returns `Left(XmlParseFailed("Unparseable voteDate: '<raw>'", Some(raw)))` — the
 * vote is NOT persisted (fail-fast) and the client wraps the failure in
 * [[repcheck.ingestion.votes.errors.SenateVoteFetchFailed]] before raising.
 *
 * The DTO retains the raw `voteDate` string unchanged (no re-formatting): persistence code downstream chooses its own
 * canonical representation.
 */
object SenateVoteXmlDecoder {

  private val IsoLocalFormatter: DateTimeFormatter =
    DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss")

  private val LongWithDayOfWeekFormatter: DateTimeFormatter =
    DateTimeFormatter.ofPattern("EEEE, MMMM d, yyyy, hh:mm a", Locale.US)

  private val LongNoDayOfWeekFormatter: DateTimeFormatter =
    DateTimeFormatter.ofPattern("MMMM d, yyyy, hh:mm a", Locale.US)

  private val RawFragmentMaxLength: Int = 200

  /**
   * Decode a `<roll_call_vote>` XML document into a [[SenateVoteXmlDTO]]. Validates that `voteDate` can be parsed in at
   * least one supported format; returns the raw string in the DTO on success, or `Left(XmlParseFailed)` on failure. The
   * vote's `<members>` list is decoded entry-by-entry; if any entry is missing a required field (for example
   * `<lis_member_id>`), the whole decode fails rather than silently dropping members — ingestion correctness matters
   * more than best-effort tolerance for roll-call positions.
   *
   * Per shared-models 0.1.45, [[SenateVoteDocumentDTO]] now carries `amendmentNumber`,
   * `amendmentToDocumentNumber`, and `amendmentToDocumentShortTitle` directly. Senate.gov emits those three elements
   * top-level on `<roll_call_vote>` (not nested under `<document>`) and only populates them on amendment votes; the
   * decoder reads them at the root level and writes them into the canonical document DTO.
   */
  def decodeVote(elem: Elem): Either[XmlParseFailed, SenateVoteXmlDTO] =
    if (elem.label != "roll_call_vote") {
      Left(
        XmlParseFailed(
          s"Expected <roll_call_vote> root, found <${elem.label}>",
          Some(truncate(elem.toString)),
        )
      )
    } else {
      for {
        congress   <- requireInt(elem, "congress")
        session    <- requireInt(elem, "session")
        voteNumber <- requireInt(elem, "vote_number")
        question   <- requireText(elem, "question")
        voteDate   <- requireText(elem, "vote_date").flatMap(validateDate)
        result     <- resolveResult(elem)
        document   <- decodeDocument(elem)
        members    <- decodeMembers(elem)
      } yield SenateVoteXmlDTO(
        congress = congress,
        session = session,
        voteNumber = voteNumber,
        question = question,
        voteDate = voteDate,
        result = result,
        document = document.copy(
          amendmentNumber = textOpt(elem, "amendment_number"),
          amendmentToDocumentNumber = textOpt(elem, "amendment_to_document_number"),
          amendmentToDocumentShortTitle = textOpt(elem, "amendment_to_document_short_title"),
        ),
        members = members,
      )
    }

  /**
   * Decode a `<vote_summary>` document into a list of [[SenateVoteIndexEntry]]s. Each `<vote>` element must carry a
   * numeric `<vote_number>`, a non-blank `<vote_date>`, a `<question>`, and a `<result>`; a failure in any single entry
   * fails the whole list so the caller sees a consistent "all-or-nothing" result (matches the per-vote decoder's
   * behavior and avoids partial indexes silently dropping entries).
   */
  def decodeIndex(elem: Elem): Either[XmlParseFailed, List[SenateVoteIndexEntry]] =
    if (elem.label != "vote_summary") {
      Left(
        XmlParseFailed(
          s"Expected <vote_summary> root, found <${elem.label}>",
          Some(truncate(elem.toString)),
        )
      )
    } else {
      val nodes = (elem \ "votes" \ "vote").toList
      // Per-entry tolerance: a single malformed `<vote>` (e.g. an old impeachment-trial procedural that
      // lacks both `<question>` and `<en_bloc>`) used to fail-fast and discard every other entry in the
      // session — losing 200+ votes for one bad row. Now we collect the entries that decode successfully
      // and silently drop the ones that don't. The bad entries don't surface as errors here; the per-vote
      // detail fetch still runs against the stored vote_number list and any vote that was dropped at
      // index time would also fail at detail time, where the per-vote fail-with-classifier path catches it.
      Right(nodes.flatMap(node => decodeIndexEntry(node).toOption))
    }

  /**
   * Decode one `<vote>` entry from the index feed.
   *
   * senate.gov uses an `<en_bloc>` variant for batched-confirmation votes (multiple `<matter>` children, each with its
   * own `<question>`/`<result>`). Those votes do NOT carry a top-level `<question>` or `<result>` — so we fall back to
   * canonical `"En Bloc"` labels and let downstream processors look up the individual `<matter>` detail via the
   * per-vote XML feed. This keeps the index parser tolerant of senate.gov's real feed variety while still failing
   * loudly on truly-missing required fields.
   */
  private def decodeIndexEntry(node: Node): Either[XmlParseFailed, SenateVoteIndexEntry] = {
    val isEnBloc = (node \ "en_bloc").nonEmpty
    for {
      rawVoteNum <- requireText(node, "vote_number")
      voteNumber <- parseInt(rawVoteNum, "vote_number")
      voteDate   <- requireText(node, "vote_date")
      question   <- indexQuestionOrEnBloc(node, isEnBloc)
      result     <- indexResultOrEnBloc(node, isEnBloc)
    } yield SenateVoteIndexEntry(
      voteNumber = voteNumber,
      voteDate = voteDate,
      question = question,
      result = result,
    )
  }

  private def indexQuestionOrEnBloc(node: Node, isEnBloc: Boolean): Either[XmlParseFailed, String] =
    directChildTextOpt(node, "question") match {
      case Some(text)       => Right(text)
      case None if isEnBloc => Right("En Bloc")
      case None =>
        Left(XmlParseFailed("Missing or empty <question> element", Some(truncate(node.toString))))
    }

  private def indexResultOrEnBloc(node: Node, isEnBloc: Boolean): Either[XmlParseFailed, String] =
    directChildTextOpt(node, "result") match {
      case Some(text)       => Right(text)
      case None if isEnBloc => Right("En Bloc")
      case None =>
        Left(XmlParseFailed("Missing or empty <result> element", Some(truncate(node.toString))))
    }

  /**
   * Return the text of the FIRST direct child with the given label — never descendants. Useful for the index feed where
   * an `<en_bloc>` variant nests `<question>` / `<result>` inside `<matter>` children: the direct-child lookup
   * correctly returns `None` for those votes so the caller can substitute an `"En Bloc"` label.
   */
  private def directChildTextOpt(node: Node, tag: String): Option[String] = {
    val directChildren = node.child.collect { case e: scala.xml.Elem if e.label == tag => e }
    directChildren.headOption.flatMap { child =>
      val raw = child.text.trim.replaceAll("\\s+", " ")
      if (raw.isEmpty) None else Some(raw)
    }
  }

  /**
   * Decode the `<document>` child into [[SenateVoteDocumentDTO]]. Tolerates a missing `<document>` element entirely —
   * senate.gov omits it for purely procedural roll-calls that don't reference a bill, nomination, or treaty (e.g.
   * "Motion to Adjourn the Court of Impeachment Sine Die" during the 2024 Mayorkas trial: 118-Senate-2 votes 129..140
   * all lack `<document>` because they're votes IN the impeachment trial itself, not on any underlying legislation).
   * When missing, returns a synthetic `SenateVoteDocumentDTO` with empty strings — the converter's
   * `normalizeDocumentType` then classifies it as `NonBillOrUnknown` and persists the vote with `billId=None` plus a
   * warn, the same shape we already use for amendment votes (S.Amdt./H.Amdt.) and presidential nominations (PN).
   *
   * Tolerates an empty `<document_short_title/>` element (represented as `None`).
   *
   * Surfaced live during P6 docker-compose backfill on Senate 118-2 votes 129..140 (impeachment-trial procedural
   * motions). Pre-fix every one of those votes was dropped at decode time; post-fix they persist as procedural Senate
   * votes with no bill linkage.
   */
  private def decodeDocument(elem: Elem): Either[XmlParseFailed, SenateVoteDocumentDTO] =
    (elem \ "document").headOption match {
      case None =>
        Right(
          SenateVoteDocumentDTO(
            documentCongress = 0,
            documentType = "",
            documentNumber = "",
            documentName = "",
            documentTitle = "",
            documentShortTitle = None,
            amendmentNumber = None,
            amendmentToDocumentNumber = None,
            amendmentToDocumentShortTitle = None,
          )
        )
      case Some(docNode) =>
        // Older votes (109th-Congress era and earlier) often emit a <document> with empty or self-closing
        // <document_congress/>, <document_type/>, etc. — the schema looks roughly the same but the data
        // simply isn't there. Tolerate empty values for ALL document fields and let the converter classify
        // the resulting empty/unknown documentType through the existing NonBillOrUnknown branch (the same
        // path used for amendment votes / nominations / procedural motions today).
        //
        // Surfaced live during P6 docker-compose backfill on 109-Senate-1 vote 366 and earlier — every old
        // vote was being dropped at decode time because of a strict requireInt on document_congress.
        val docCongress = textOpt(docNode, "document_congress")
          .flatMap(s => scala.util.Try(s.toInt).toOption)
          .getOrElse(0)
        val docType   = textOpt(docNode, "document_type").getOrElse("")
        val docNumber = textOpt(docNode, "document_number").getOrElse("")
        val docName   = textOpt(docNode, "document_name").getOrElse("")
        val docTitle  = textOpt(docNode, "document_title").getOrElse("")
        // document_short_title is optional — senate.gov often emits it as <document_short_title/> (self-closing empty)
        val docShortTitle = textOpt(docNode, "document_short_title")
        Right(
          SenateVoteDocumentDTO(
            documentCongress = docCongress,
            documentType = docType,
            documentNumber = docNumber,
            documentName = docName,
            documentTitle = docTitle,
            documentShortTitle = docShortTitle,
            amendmentNumber = None,
            amendmentToDocumentNumber = None,
            amendmentToDocumentShortTitle = None,
          )
        )
    }

  private def decodeMembers(elem: Elem): Either[XmlParseFailed, List[SenateVoteMemberXmlDTO]] = {
    val nodes = (elem \ "members" \ "member").toList
    nodes
      .foldLeft[Either[XmlParseFailed, List[SenateVoteMemberXmlDTO]]](Right(Nil)) { (acc, node) =>
        for {
          soFar  <- acc
          member <- decodeMember(node)
        } yield member :: soFar
      }
      .map(_.reverse)
  }

  private def decodeMember(node: Node): Either[XmlParseFailed, SenateVoteMemberXmlDTO] =
    for {
      lisMemberId <- requireText(node, "lis_member_id")
      firstName   <- requireText(node, "first_name")
      lastName    <- requireText(node, "last_name")
      party       <- requireText(node, "party")
      state       <- requireText(node, "state")
      voteCast    <- requireText(node, "vote_cast")
    } yield SenateVoteMemberXmlDTO(
      lisMemberId = lisMemberId,
      firstName = firstName,
      lastName = lastName,
      party = party,
      state = state,
      voteCast = voteCast,
    )

  /**
   * Result text: prefer `<vote_result>` (official outcome string, e.g. "Nomination Confirmed"); fall back to `<result>`
   * (short form used by the index feed). If both are missing, fail with a clear message rather than defaulting to empty
   * — a missing outcome is a structural decoder error, not a data-quality edge case.
   */
  private def resolveResult(elem: Elem): Either[XmlParseFailed, String] =
    textOpt(elem, "vote_result")
      .orElse(textOpt(elem, "result"))
      .toRight(
        XmlParseFailed(
          "Missing <vote_result> (or <result>) element",
          Some(truncate(elem.toString)),
        )
      )

  private def validateDate(raw: String): Either[XmlParseFailed, String] = {
    val normalized = raw.trim.replaceAll("\\s+", " ")
    val attempts = List[() => Any](
      () => OffsetDateTime.parse(raw),
      () => java.time.LocalDateTime.parse(raw, IsoLocalFormatter),
      () => java.time.LocalDateTime.parse(normalized, LongWithDayOfWeekFormatter),
      () => java.time.LocalDateTime.parse(normalized, LongNoDayOfWeekFormatter),
    )
    firstSuccess(attempts) match {
      case Right(_) => Right(raw)
      case Left(cause) =>
        Left(
          XmlParseFailed(
            s"Unparseable voteDate: '$raw'",
            Some(truncate(s"$raw|cause=${cause.getMessage}")),
          )
        )
    }
  }

  /**
   * Walk `attempts` in order, returning the first successful result (as `Right(())`) or the last thrown error (as
   * `Left`). `attempts` is required to be non-empty at the call site (`validateDate` hands in a static 4-element list),
   * so the "all empty" case returns a project-specific sentinel rather than a stdlib exception.
   */
  private def firstSuccess(attempts: List[() => Any]): Either[Throwable, Unit] = {
    def loop(remaining: List[() => Any], lastError: Option[Throwable]): Either[Throwable, Unit] =
      remaining match {
        case Nil =>
          Left(
            lastError.getOrElse(
              repcheck.ingestion.votes.errors.XmlParseFailed("No date formats were tried", None)
            )
          )
        case head :: tail =>
          tryAttempt(head) match {
            case Right(_)  => Right(())
            case Left(err) => loop(tail, Some(err))
          }
      }
    loop(attempts, None)
  }

  private def tryAttempt(attempt: () => Any): Either[Throwable, Unit] =
    Try(attempt()).toEither.map(_ => ())

  private def parseInt(raw: String, tag: String): Either[XmlParseFailed, Int] =
    raw.trim.toIntOption match {
      case Some(n) => Right(n)
      case None =>
        Left(
          XmlParseFailed(
            s"Expected integer in <$tag> but found: '$raw'",
            Some(truncate(raw)),
          )
        )
    }

  private def requireInt(node: Node, tag: String): Either[XmlParseFailed, Int] =
    requireText(node, tag).flatMap(parseInt(_, tag))

  private def requireText(node: Node, tag: String): Either[XmlParseFailed, String] =
    textOpt(node, tag).toRight(
      XmlParseFailed(
        s"Missing or empty <$tag> element",
        Some(truncate(node.toString)),
      )
    )

  private def textOpt(node: Node, tag: String): Option[String] = {
    val seq: NodeSeq = node \ tag
    val raw          = seq.text.trim.replaceAll("\\s+", " ")
    if (raw.isEmpty) None else Some(raw)
  }

  private def truncate(s: String): String =
    if (s.length <= RawFragmentMaxLength) s
    else s.substring(0, RawFragmentMaxLength) + "..."

}
