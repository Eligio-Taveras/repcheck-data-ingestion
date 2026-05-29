package repcheck.members.committees.client

import scala.annotation.tailrec
import scala.util.matching.Regex

import repcheck.members.committees.model.CdirAssignment

/**
 * Parses a GovInfo Congressional Directory committee-listing granule (STANDING/SELECT/SPECIAL/JOINT COMMITTEES, plain
 * text) into committee assignments.
 *
 * The listing is a centered, two-column directory: a committee name header, address/phone/URL lines, then member
 * entries "First M. Last, of State[, Chair]." laid across two columns with names wrapping mid-cell. We:
 *   1. split into per-committee blocks, anchoring headers on the DB's known committee names (so wrapped fragments like
 *      "Massachusetts." are never mistaken for headers) plus "Subcommittee on …" lines; 2. split each line into
 *      left/right columns at the inter-column gap and accumulate each column separately, so a wrapped state rejoins its
 *      name within the same column; 3. scan each column for "Name, of <State>" anchored on the known finite state set
 *      (period-independent, since the Chair/Vice Chair leaders are not period-terminated).
 */
object CdirCommitteeListingParser {

  private val OfMarker: String = ", of "

  private val RoleLookahead: Regex =
    raw"""^,?\s*(Vice Chairman|Vice Chair|Chairman|Chair|Ranking Minority Member|Ranking Member|Ranking)\b""".r

  private val RoleWords: Set[String] =
    Set("chair", "vice", "chairman", "ranking", "member", "minority", "cochair", "co-chair")

  private val RoleAliases: Map[String, String] = Map(
    "chairman"                -> "Chairman",
    "chair"                   -> "Chairman",
    "vice chair"              -> "Vice Chairman",
    "vice chairman"           -> "Vice Chairman",
    "ranking"                 -> "Ranking Member",
    "ranking member"          -> "Ranking Member",
    "ranking minority member" -> "Ranking Member",
  )

  private val Suffixes: Set[String] = Set("jr.", "jr", "sr.", "sr", "ii", "iii", "iv")

  final private case class Block(name: String, isSub: Boolean, left: String, right: String)

  def parse(text: String, knownCommitteeNames: Set[String], stateNames: Set[String]): List[CdirAssignment] = {
    val normalizedKnown = knownCommitteeNames.map(normalizeCommittee).filter(_.nonEmpty)
    val statesByLenDesc = stateNames.toList.sortBy(-_.length)
    val lines           = text.split("\\r?\\n", -1).toList.map(_.replaceAll("^\\s+|\\s+$", ""))

    val (acc, last) = lines.foldLeft((List.empty[Block], Option.empty[Block])) {
      case ((done, cur), line) =>
        if (line.isEmpty) (done, cur)
        else if (isSubcommitteeHeader(line)) (cur.toList ::: done, Some(Block(line, isSub = true, "", "")))
        else if (normalizedKnown.contains(normalizeCommittee(line)))
          (cur.toList ::: done, Some(Block(line, isSub = false, "", "")))
        else
          cur match {
            case None => (done, None)
            case Some(b) =>
              val (l, r) = splitColumns(line)
              (done, Some(b.copy(left = s"${b.left} $l", right = if (r.nonEmpty) s"${b.right} $r" else b.right)))
          }
    }

    (last.toList ::: acc).reverse.flatMap { b =>
      (extractColumn(b.left, statesByLenDesc) ++ extractColumn(b.right, statesByLenDesc))
        .map(_.copy(committeeName = b.name, isSubcommittee = b.isSub))
    }
  }

  private def isSubcommitteeHeader(line: String): Boolean =
    line.toLowerCase.startsWith("subcommittee on")

  /**
   * Split a stripped line into (leftCell, rightCell) at the first run of 2+ spaces; rightCell is "" if single-column.
   */
  private def splitColumns(stripped: String): (String, String) =
    raw"\s{2,}".r.findFirstMatchIn(stripped) match {
      case Some(m) => (stripped.substring(0, m.start).trim, stripped.substring(m.end).trim)
      case None    => (stripped.trim, "")
    }

  /**
   * Walk the collapsed column anchored on ", of <State>". The name is the gap between the previous entry's state end
   * and the current marker — this avoids the trailing-"State." and period-less-"Chair" of the preceding entry leaking
   * into the next name.
   */
  private def extractColumn(columnText: String, statesByLenDesc: List[String]): List[CdirAssignment] = {
    val s = columnText.replaceAll("\\s+", " ").trim

    @tailrec
    def scan(cursor: Int, acc: List[CdirAssignment]): List[CdirAssignment] = {
      val idx = s.indexOf(OfMarker, cursor)
      if (idx < 0) acc.reverse
      else {
        val afterOf = s.substring(idx + OfMarker.length)
        matchState(afterOf, statesByLenDesc) match {
          case Some((state, rest)) =>
            val entry = cleanName(s.substring(cursor, idx)).map {
              case (first, last) =>
                CdirAssignment("", isSubcommittee = false, first, last, state, roleAfter(rest))
            }
            scan(idx + OfMarker.length + state.length, entry.toList ::: acc)
          case None =>
            scan(idx + OfMarker.length, acc)
        }
      }
    }

    scan(0, Nil)
  }

  /** Greedily match the longest known state name at the start of `after`; returns (canonicalState, remainder). */
  private def matchState(after: String, statesByLenDesc: List[String]): Option[(String, String)] =
    statesByLenDesc
      .find(st => after.regionMatches(true, 0, st, 0, st.length))
      .map(st => (st, after.substring(st.length)))

  private def roleAfter(rest: String): Option[String] =
    RoleLookahead.findFirstMatchIn(rest).flatMap(m => RoleAliases.get(m.group(1).toLowerCase.trim))

  /**
   * Reduce the raw gap text to (firstName, lastName). The person's name is the run of name-like tokens immediately
   * before ", of", so we collect tokens BACKWARD while they look like name parts (uppercase-initial, not a role word),
   * stopping at the first non-name token — a leftover "Chair", a "." from the previous state, or address/URL junk.
   */
  private def cleanName(raw: String): Option[(String, String)] = {
    val deNick     = raw.replaceAll("``[^']*''", "").replaceAll("\"[^\"]*\"", "").replaceAll("\\s+", " ").trim
    val nameTokens = deNick.split(" ").toList.filter(_.nonEmpty).reverse.takeWhile(isNameToken).reverse
    nameTokens match {
      case Nil | _ :: Nil => None
      case _ =>
        val (last, firstParts) = dropTrailingSuffix(nameTokens)
        firstParts.headOption.map(first => first -> last)
    }
  }

  private def isNameToken(token: String): Boolean = {
    val t = token.replace(",", "")
    t.nonEmpty && t.headOption.exists(_.isUpper) && !RoleWords.contains(t.toLowerCase) && !t.contains("/")
  }

  private def dropTrailingSuffix(tokens: List[String]): (String, List[String]) =
    tokens.reverse match {
      case suffix :: last :: rest if Suffixes.contains(suffix.toLowerCase.replace(",", "")) =>
        (last.replace(",", ""), rest.reverse)
      case last :: rest => (last.replace(",", ""), rest.reverse)
      case Nil          => ("", Nil)
    }

  /** Normalization shared with the loader so CDIR header text and DB committee names key identically. */
  def normalizeCommittee(s: String): String =
    s.toLowerCase.replaceAll("committee", "").replaceAll("[^a-z0-9 ]", " ").replaceAll("\\s+", " ").trim

}
