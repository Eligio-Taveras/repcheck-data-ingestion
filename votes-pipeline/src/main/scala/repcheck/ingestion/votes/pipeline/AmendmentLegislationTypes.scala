package repcheck.ingestion.votes.pipeline

import repcheck.shared.models.congress.amendment.AmendmentType

/**
 * Detects amendment-typed `legislationType` strings on Congress.gov vote responses (House path). Senate-prefixed
 * amendment types (`SAMDT`, `SUAMDT`) CAN appear in House vote contexts when crossing chambers — the House reports
 * roll-calls on Senate amendments using the Senate type code. Set membership against the parsed [[AmendmentType]] enum
 * so we don't drift if shared-models adds/removes a variant.
 */
private[votes] object AmendmentLegislationTypes {

  /**
   * `true` iff the given raw string parses to an [[AmendmentType]] (HAMDT/SAMDT/SUAMDT). Case-insensitive —
   * Congress.gov returns upper-case in vote responses but accept either to match `AmendmentType.fromString`'s behavior.
   */
  def isAmendmentType(raw: String): Boolean =
    AmendmentType.fromString(raw).isRight

  /**
   * Parse the optional `legislationType` field on a vote DTO into an [[AmendmentType]] when present and
   * amendment-typed. Returns `None` for non-amendment, missing, or unrecognized values — bill-typed inputs (HR, S,
   * etc.) are also `None`, so callers fall back to the existing bill-lookup path on `None`.
   */
  def parseAmendmentType(raw: Option[String]): Option[AmendmentType] =
    raw.flatMap(s => AmendmentType.fromString(s).toOption)

  /**
   * Build the canonical amendment natural key (`{congress}-{TYPE}-{number}`) from raw vote-DTO fields. Mirrors
   * [[VoteNaturalKeys.houseBill]]'s shape but for amendments — the parsed [[AmendmentType]] is rendered to its
   * canonical upper-case `toString` so the key lines up with what
   * [[repcheck.ingestion.amendments.persistence.AmendmentRepository.upsertPlaceholder]] expects.
   */
  def buildAmendmentNaturalKey(congress: Int, amendmentType: AmendmentType, number: String): String =
    s"${congress.toString}-${amendmentType.toString}-$number"

}
