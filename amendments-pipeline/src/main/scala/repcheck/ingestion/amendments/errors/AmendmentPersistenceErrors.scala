package repcheck.ingestion.amendments.errors

/**
 * Raised when a persistence operation against the `amendments` table fails for a row identified by its natural key
 * (`{congress}-{amendmentType}-{number}`). The wrapped `cause` carries the underlying SQL/driver exception so callers
 * have the raw context for retry classification (`ErrorClassifier`) without losing the natural-key crumb.
 */
final case class AmendmentUpsertFailed(
  naturalKey: String,
  detail: String,
  cause: Throwable,
) extends Exception(s"Failed to upsert amendment $naturalKey: $detail", cause)

/**
 * Raised by `AmendmentRepository.upsertPlaceholder` when the supplied natural key cannot be parsed into the `(congress,
 * amendmentType, number)` triple required to populate a placeholder row. Natural keys must match the format
 * `"<congress>-<AMENDMENT_TYPE>-<number>"` — e.g. `"117-SAMDT-2137"` — with `congress` parseable as `Int` and
 * `AMENDMENT_TYPE` a recognized [[repcheck.shared.models.congress.amendment.AmendmentType]] apiValue
 * (case-insensitive).
 *
 * Typical causes: an upstream feed emitted a malformed identifier, the `AmendmentType` enum is missing a new variant
 * the API introduced, or a caller constructed a natural key by hand without going through the canonical builder.
 */
final case class InvalidAmendmentNaturalKey(
  naturalKey: String,
  detail: String,
) extends Exception(s"Invalid amendment natural key '$naturalKey': $detail")
