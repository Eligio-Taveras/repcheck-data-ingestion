package repcheck.ingestion.bills.textcheck.selection

import repcheck.shared.models.congress.dto.bill.{FormatDTO, TextVersionDTO}

object TextVersionSelector {

  private val FormatPriority: Map[String, Int] = Map(
    "Formatted Text" -> 0,
    "Formatted XML"  -> 1,
    "PDF"            -> 2,
  )

  /**
   * Maps Congress.gov version type strings (descriptive long-form OR direct short codes) to the canonical short codes
   * persisted in the `text_version_code_type` enum. The DB enum is the source of truth for valid codes — anything that
   * doesn't have a mapping here will be returned as `None` from [[toVersionCode]] and the caller skips the version row
   * rather than leaking unmapped strings to the DB (which would fail enum validation at INSERT time).
   *
   * Coverage is empirically grown as new Congress.gov long-forms surface in production. See db-migrations 016, 021, and
   * 030 for the parallel batches of enum-value additions; this map and the enum stay in lockstep.
   */
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
    "Engrossed Amendment House"    -> "EAH",
    "Engrossed Amendment Senate"   -> "EAS",
    "Enrolled Bill"                -> "ENR",
    "Considered and Passed House"  -> "CPH",
    "Considered and Passed Senate" -> "CPS",
    "Public Law"                   -> "PL",
    "Private Law"                  -> "PRL",
    "Public Print"                 -> "PP",
    "Printed as Passed"            -> "PP",
    "Received in Senate"           -> "RDS",
    "Received in House"            -> "RDH",
    "Agreed to Senate"             -> "ATS",
    "Agreed to House"              -> "ATH",
    "Placed on Calendar Senate"    -> "PCS",
    "Placed on Calendar House"     -> "PCH",
    "Reported to Senate"           -> "RTS",
    "Reported to House"            -> "RTH",
    "Reference Change House"       -> "RCH",
    "Reference Change Senate"      -> "RCS",
    "Referral Instructions House"  -> "RIH",
    "Referral Instructions Senate" -> "RIS",
    "Laid on Table in House"       -> "LTH",
    "Laid on Table in Senate"      -> "LTS",
    // Short codes the API sometimes sends directly — passthrough
    "ATH" -> "ATH",
    "ATS" -> "ATS",
    "CPH" -> "CPH",
    "CPS" -> "CPS",
    "EAH" -> "EAH",
    "EAS" -> "EAS",
    "EH"  -> "EH",
    "ENR" -> "ENR",
    "ES"  -> "ES",
    "IH"  -> "IH",
    "IS"  -> "IS",
    "LTH" -> "LTH",
    "LTS" -> "LTS",
    "PCH" -> "PCH",
    "PCS" -> "PCS",
    "PL"  -> "PL",
    "PP"  -> "PP",
    "PRL" -> "PRL",
    "RCH" -> "RCH",
    "RCS" -> "RCS",
    "RDH" -> "RDH",
    "RDS" -> "RDS",
    "RFH" -> "RFH",
    "RFS" -> "RFS",
    "RH"  -> "RH",
    "RIH" -> "RIH",
    "RIS" -> "RIS",
    "RS"  -> "RS",
    "RTH" -> "RTH",
    "RTS" -> "RTS",
  )

  /**
   * Maps a Congress.gov-emitted version type string to the canonical enum short code, or `None` when the value isn't
   * known. Returning `None` (instead of falling back to the unmapped input) lets the caller — the
   * `BillTextAvailabilityChecker` — emit a `ProcessingResult.Failed("MissingVersionCode")` and skip persistence rather
   * than INSERT a long-form like `"Public Print"` that would violate the `text_version_code_type` enum check.
   *
   * Pipeline behaviour for unmapped inputs surfaces them via:
   *   - the Failed processing result (visible in logs and processing_results audit)
   *   - the next step's no-row outcome (no `bill_text_versions` row for that bill+version)
   *
   * When a new value surfaces, extend this map AND if the short code is genuinely new, add it to the
   * `text_version_code_type` enum via a follow-up db-migrations migration in the same batch pattern as 016 / 021 / 030.
   */
  private[selection] def toVersionCode(descriptive: String): Option[String] =
    VersionTypeToCode.get(descriptive)

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
              versionType = version.type_.flatMap(toVersionCode),
              formatType = format.type_,
              url = format.url,
            )
        }
    }
  }

}
