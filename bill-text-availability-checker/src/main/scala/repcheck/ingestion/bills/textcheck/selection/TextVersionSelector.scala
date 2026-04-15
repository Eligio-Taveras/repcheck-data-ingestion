package repcheck.ingestion.bills.textcheck.selection

import repcheck.shared.models.congress.dto.bill.{FormatDTO, TextVersionDTO}

object TextVersionSelector {

  private val FormatPriority: Map[String, Int] = Map(
    "Formatted Text" -> 0,
    "Formatted XML"  -> 1,
    "PDF"            -> 2,
  )

  /** Maps Congress.gov version type strings (descriptive or short) to the short codes used in the DB enum. */
  private val VersionTypeToCode: Map[String, String] = Map(
    // Long-form descriptive → short code
    "Introduced in House"          -> "IH",
    "Introduced in Senate"         -> "IS",
    "Reported in House"            -> "RH",
    "Reported in Senate"           -> "RS",
    "Referred in House"            -> "RFH",
    "Referred in Senate"           -> "RFS",
    "Engrossed in House"           -> "EH",
    "Engrossed in Senate"          -> "ES",
    "Enrolled Bill"                -> "ENR",
    "Considered and Passed House"  -> "CPH",
    "Considered and Passed Senate" -> "CPS",
    "Public Law"                   -> "PL",
    "Received in Senate"           -> "RDS",
    "Received in House"            -> "RDH",
    "Agreed to Senate"             -> "ATS",
    "Agreed to House"              -> "ATH",
    "Placed on Calendar Senate"    -> "PCS",
    "Placed on Calendar House"     -> "PCH",
    "Reported to Senate"           -> "RTS",
    "Reported to House"            -> "RTH",
    "Printed as Passed"            -> "PP",
    // Short codes the API sometimes sends directly
    "PCS" -> "PCS",
    "PCH" -> "PCH",
    "PL"  -> "PL",
    "RDS" -> "RDS",
    "RDH" -> "RDH",
    "RTS" -> "RTS",
    "RTH" -> "RTH",
    "ATS" -> "ATS",
    "ATH" -> "ATH",
    "PP"  -> "PP",
  )

  /** Returns the short code for a known descriptive string, or the input unchanged if not recognized. */
  private[selection] def toVersionCode(descriptive: String): String =
    VersionTypeToCode.getOrElse(descriptive, descriptive)

  final case class SelectedVersion(
    date: Option[String],
    versionType: Option[String],
    formatType: String,
    url: String,
  )

  def selectBestVersion(versions: List[TextVersionDTO]): Option[SelectedVersion] = {
    val candidates: List[(Int, TextVersionDTO, FormatDTO)] = for {
      version  <- versions
      formats  <- version.formats.toList
      format   <- formats
      priority <- FormatPriority.get(format.type_).toList
    } yield (priority, version, format)

    if (candidates.isEmpty) {
      None
    } else {
      // Primary: latest date wins (newer bills have updated text)
      // Secondary: within the same date, use format priority as tiebreaker
      candidates
        .sortBy {
          case (priority, version, _) =>
            (version.date.getOrElse(""), -priority)
        }
        .lastOption
        .map {
          case (_, version, format) =>
            SelectedVersion(
              date = version.date,
              versionType = version.type_.map(toVersionCode),
              formatType = format.type_,
              url = format.url,
            )
        }
    }
  }

}
