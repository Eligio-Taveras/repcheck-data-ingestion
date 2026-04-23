package repcheck.ingestion.bills.common.errors

final case class BillUpsertFailed(
  billId: String,
  detail: String,
  cause: Throwable,
) extends Exception(s"Failed to upsert bill $billId: $detail", cause)

final case class BillArchiveFailed(
  billId: String,
  detail: String,
  cause: Throwable,
) extends Exception(s"Failed to archive bill history for $billId: $detail", cause)

final case class BillCosponsorReplaceFailed(
  billId: Long,
  detail: String,
  cause: Throwable,
) extends Exception(s"Failed to replace cosponsors for bill $billId: $detail", cause)

final case class BillSubjectReplaceFailed(
  billId: Long,
  detail: String,
  cause: Throwable,
) extends Exception(s"Failed to replace subjects for bill $billId: $detail", cause)

final case class BillTextVersionInsertFailed(
  billId: Long,
  detail: String,
  cause: Throwable,
) extends Exception(s"Failed to insert text version for bill $billId: $detail", cause)

/**
 * Raised by `BillRepository.upsertPlaceholder` when the supplied natural key cannot be parsed into the composite
 * `(congress, bill_type, number)` keys the `bills` table indexes on. Natural keys must match the format produced by
 * `repcheck.shared.models.congress.dto.conversions.BillConversions.buildBillNaturalKey` — i.e.
 * `"<congress>-<BILL_TYPE>-<number>"` with `congress`/`number` parseable as `Int` and `BILL_TYPE` a recognized
 * `BillType` apiValue (case-insensitive).
 *
 * Typical causes: a feed emitted a malformed identifier, the `BillType` enum is missing a new variant the API
 * introduced, or a caller constructed a natural key by hand without going through `buildBillNaturalKey`.
 */
final case class InvalidBillNaturalKey(
  naturalKey: String,
  detail: String,
) extends Exception(s"Invalid bill natural key '$naturalKey': $detail")
