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
