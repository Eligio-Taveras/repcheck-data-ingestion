package repcheck.ingestion.amendments.textcheck.selection

import repcheck.shared.models.congress.dto.amendment.{AmendmentFormatDTO, AmendmentTextItemDTO}

/**
 * Selects every NEW (versionTypeCode, formatType) tuple to emit `amendment.text.available` events for, given an
 * amendment's upstream text versions plus the set of (versionTypeCode, formatType) tuples already ingested for that
 * amendment.
 *
 * Per Q8 of the §7.5 plan: emit events for ALL new versions (Submitted + any subsequent Modified both get ingested as
 * historical evidence), not just the latest canonical.
 *
 * Supported upstream format types:
 *   - "HTML" — preferred (lossless extraction via HtmlStreamExtractor downstream)
 *   - "PDF" — fallback (PdfStreamExtractor; lossier)
 *
 * Verified against the live Congress.gov endpoint (e.g. `/v3/amendment/117/samdt/2137/text`) and codified in
 * `repcheck-db-migrations` 037: amendment text granules are emitted ONLY as `"PDF"` or `"HTML"` (no `"TXT"`,
 * `"Formatted Text"`, or `"Formatted XML"` — those are bill-side variants only). Bills do receive `"Formatted XML"`
 * (structured legislative XML), but the amendment endpoint does not — so there is no XML granule to drop here. An
 * upstream `formats[].type` that doesn't match `"HTML"` or `"PDF"` is treated as unsupported and skipped, keeping
 * downstream extractors strictly aligned with what this checker actually emits.
 */
object AmendmentTextVersionSelector {

  // Format priority — HTML preferred over PDF. Verified upstream values are exactly these two; anything else is
  // unsupported and skipped (see object Scaladoc for verification details).
  private val FormatPriority: Map[String, Int] = Map(
    "HTML" -> 0,
    "PDF"  -> 1,
  )

  // Mapping from upstream `type` field to the canonical short codes persisted in the
  // `amendment_text_version_code_type` enum (per L3 — distinct from bill-side `text_version_code_type`).
  private val VersionTypeToCode: Map[String, String] = Map(
    "Submitted" -> "SUB",
    "Modified"  -> "MOD",
  )

  /**
   * Map an upstream `type` string (e.g. `Some("Submitted")`) to its short enum code. `None` input or an unrecognized
   * value returns `None`; the caller skips that version rather than emitting an unmapped code that would fail enum
   * validation downstream.
   */
  def versionTypeCode(rawType: Option[String]): Option[String] =
    rawType.flatMap(VersionTypeToCode.get)

  /**
   * Compute the diff between upstream and `existing`. Each output tuple is one new (`AmendmentTextItemDTO`,
   * `AmendmentFormatDTO`) pair the caller emits as a separate `AmendmentTextAvailableEvent`.
   *
   * Algorithm:
   *   1. For each upstream version, derive `versionTypeCode` (skip versions where it's `None`). 2. Filter to that
   *      version's formats with a known `FormatPriority` entry. The amendment endpoint emits only `"HTML"` and `"PDF"`;
   *      any other type is treated as unsupported and skipped. 3. Within those, pick the highest-priority format (HTML
   *      beats PDF). 4. Subtract any (versionTypeCode, formatType) tuple already in `existing`. 5. Return the remaining
   *      tuples.
   *
   * `existing` is a list of `(versionTypeCode, formatType)` pairs read from `amendment_text_versions` (the §7.6 table)
   * for this amendment. `AmendmentTextVersionDO` is intentionally NOT used here — that lives in §7.6's scope. A minimal
   * tuple representation keeps this file independent of the not-yet-built §7.6 model.
   */
  def selectAllNewVersions(
    upstream: List[AmendmentTextItemDTO],
    existing: List[(String, String)],
  ): List[(AmendmentTextItemDTO, AmendmentFormatDTO)] = {
    val existingSet: Set[(String, String)] = existing.toSet

    upstream.flatMap { item =>
      versionTypeCode(item.`type`) match {
        case None => List.empty[(AmendmentTextItemDTO, AmendmentFormatDTO)]
        case Some(code) =>
          val supportedFormats: List[(Int, AmendmentFormatDTO)] =
            item.formats.flatMap(f => FormatPriority.get(f.`type`).map(p => (p, f)))

          // Pick highest-priority (lowest score) supported format, if any.
          // Wart.IterableOps blocks .minBy / .head, so fold and pick the lower-priority entry.
          val bestFormatOpt: Option[AmendmentFormatDTO] =
            supportedFormats
              .foldLeft(Option.empty[(Int, AmendmentFormatDTO)]) {
                case (None, candidate) => Some(candidate)
                case (Some((bestP, bestF)), (p, f)) =>
                  if (p < bestP) { Some((p, f)) }
                  else { Some((bestP, bestF)) }
              }
              .map(_._2)

          bestFormatOpt match {
            case None => List.empty[(AmendmentTextItemDTO, AmendmentFormatDTO)]
            case Some(fmt) =>
              if (existingSet.contains((code, fmt.`type`))) {
                List.empty[(AmendmentTextItemDTO, AmendmentFormatDTO)]
              } else {
                List((item, fmt))
              }
          }
      }
    }
  }

}
