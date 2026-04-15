package repcheck.ingestion.bills.metadata.errors

final case class BillProcessingFailed(
  billId: String,
  detail: String,
) extends Exception(s"Failed to process bill $billId: $detail")
