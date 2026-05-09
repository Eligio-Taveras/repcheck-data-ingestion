package repcheck.ingestion.amendments.text.persistence

import repcheck.ingestion.amendments.text.errors.UnsupportedAmendmentTextVersionCode

/**
 * Maps between the wire-format short codes used by §7.5's `amendment.text.available` event payload (`"SUB"` / `"MOD"`)
 * and the spelled-out values stored in the `amendment_text_version_code_type` enum (`"Submitted"` / `"Modified"`). This
 * boundary lives here rather than at the event layer because the database shape was set in db-migrations 037
 * (`Submitted` / `Modified`) and the §7.5 selector independently chose the short-code form for the wire to match the
 * §7.6 spec's L3 directive on a dedicated enum vocabulary.
 *
 * Pure object — no IO. Unrecognized codes raise [[UnsupportedAmendmentTextVersionCode]] (Systemic) so the processor can
 * fail fast rather than silently swallow an unknown value.
 */
private[text] object AmendmentTextVersionTypeMapping {

  private val WireToStoredMap: Map[String, String] = Map(
    "SUB" -> "Submitted",
    "MOD" -> "Modified",
  )

  /**
   * Translate a `versionTypeCode` from the wire event to the value that should be inserted into the `version_type`
   * column. Throws [[UnsupportedAmendmentTextVersionCode]] for unknown codes.
   */
  def wireToStored(wire: String): Either[UnsupportedAmendmentTextVersionCode, String] =
    WireToStoredMap.get(wire).toRight(UnsupportedAmendmentTextVersionCode(wire))

  /**
   * Inverse mapping for tests and analysis code that needs to surface the wire value alongside a stored row. Returns
   * `None` for stored values not in the map (defensive — db-migration 037 is the single source of truth for the enum,
   * but a future addition there shouldn't crash readers).
   */
  def storedToWire(stored: String): Option[String] =
    WireToStoredMap.collectFirst { case (wire, s) if s == stored => wire }

}
