package repcheck.members.committees.client

import scala.util.Try

import repcheck.members.committees.model.HistoricalAssignmentRow

/**
 * Parses the canonical historical committee-assignment file. Tab-separated (not comma) so committee names containing
 * commas need no quoting. Columns, in order:
 *
 * congress chamber committee_code committee_name committee_type bioguide_id role rank
 *
 * `congress`, `chamber`, `committee_code`, and `bioguide_id` are required; the rest may be blank (parsed as None).
 */
object HistoricalAssignmentTsvReader {

  val Header: String =
    "congress\tchamber\tcommittee_code\tcommittee_name\tcommittee_type\tbioguide_id\trole\trank"

  private val ColumnCount = 8

  /** True when the line is the header row (allows callers to skip it without hardcoding the position). */
  def isHeader(line: String): Boolean = line.trim == Header

  def parseLine(line: String): Either[String, HistoricalAssignmentRow] = {
    // -1 keeps trailing empty fields so a blank rank column still yields a slot.
    val cols = line.split("\t", -1)
    if (cols.length != ColumnCount) {
      Left(s"expected $ColumnCount tab-separated columns, got ${cols.length.toString}")
    } else {
      val congressRaw = cols(0).trim
      val chamber     = cols(1).trim
      val code        = cols(2).trim
      val bioguide    = cols(5).trim
      for {
        congress <- Try(congressRaw.toInt).toEither.left.map(_ => s"invalid congress '$congressRaw'")
        _        <- if (chamber.nonEmpty) Right(()) else Left("blank chamber")
        _        <- if (code.nonEmpty) Right(()) else Left("blank committee_code")
        _        <- if (bioguide.nonEmpty) Right(()) else Left("blank bioguide_id")
        rank     <- parseOptionalInt(cols(7).trim)
      } yield HistoricalAssignmentRow(
        congress = congress,
        chamber = chamber,
        committeeCode = code,
        committeeName = blankToNone(cols(3)),
        committeeType = blankToNone(cols(4)),
        bioguideId = bioguide,
        role = blankToNone(cols(6)),
        rank = rank,
      )
    }
  }

  private def parseOptionalInt(raw: String): Either[String, Option[Int]] =
    if (raw.isEmpty) Right(None)
    else Try(raw.toInt).toEither.left.map(_ => s"invalid rank '$raw'").map(Some(_))

  private def blankToNone(raw: String): Option[String] = {
    val t = raw.trim
    if (t.isEmpty) None else Some(t)
  }

}
