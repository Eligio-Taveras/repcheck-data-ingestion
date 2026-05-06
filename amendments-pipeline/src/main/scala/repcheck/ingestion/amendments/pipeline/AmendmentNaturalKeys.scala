package repcheck.ingestion.amendments.pipeline

import repcheck.shared.models.congress.dto.amendment.{AmendedAmendmentDTO, AmendedBillDTO, AmendmentListItemDTO}

/**
 * Natural-key builders for amendments and their referenced parents. Keeps the `{congress}-{TYPE}-{number}` shape in one
 * place so the processor, repository, and tests all agree on casing (`TYPE` upper-case for the storage row's
 * `natural_key` column).
 *
 * Parent references on amendment detail DTOs come back with optional `congress` / `type` / `number` fields — the
 * `Option`-aware `parentAmendmentNaturalKey` returns `None` when any leg is missing rather than producing a garbage key
 * that the repository would later reject as malformed.
 */
private[pipeline] object AmendmentNaturalKeys {

  def fromListItem(item: AmendmentListItemDTO): String =
    s"${item.congress.toString}-${item.amendmentType.getOrElse("UNKNOWN").toUpperCase}-${item.number}"

  /**
   * Build the parent-amendment natural key from an `amendedAmendment` DTO sub-object. Returns `None` when any of the
   * required path components (`congress`, `type`, `number`) is missing — the upstream DTO permits partial values, and a
   * malformed key would only get rejected by the repository's parse step further down.
   */
  def parentAmendmentNaturalKey(parent: AmendedAmendmentDTO): Option[String] =
    for {
      congress      <- parent.congress
      amendmentType <- parent.amendmentType.map(_.toUpperCase)
      number        <- parent.number
    } yield s"${congress.toString}-$amendmentType-$number"

  /**
   * Build the amended-bill natural key. Returns `None` when any field is missing so the caller can short-circuit the
   * placeholder + lookup round-trip without raising.
   */
  def amendedBillNaturalKey(amended: AmendedBillDTO): Option[String] =
    for {
      congress <- amended.congress
      billType <- amended.billType.map(_.toUpperCase)
      number   <- amended.number
    } yield s"${congress.toString}-$billType-$number"

}
