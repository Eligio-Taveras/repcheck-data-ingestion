package repcheck.ingestion.amendments.text.errors

/**
 * Raised when an upstream `amendment.text.available` event carries a `versionTypeCode` the pipeline doesn't know how to
 * persist. The event-side selector (§7.5) maps `"Submitted" -> "SUB"` and `"Modified" -> "MOD"`; the database enum
 * (db-migrations 037 — `amendment_text_version_code_type`) stores them spelled out as `"Submitted"` / `"Modified"`.
 * Unrecognized codes are Systemic — retrying the same code can never succeed.
 */
final case class UnsupportedAmendmentTextVersionCode(
  versionTypeCode: String
) extends Exception(
      s"Unsupported amendment text versionTypeCode '$versionTypeCode'. Expected 'SUB' or 'MOD' (per §7.5 selector)."
    )
