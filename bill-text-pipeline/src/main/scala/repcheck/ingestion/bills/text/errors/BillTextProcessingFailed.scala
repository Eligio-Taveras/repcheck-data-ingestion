package repcheck.ingestion.bills.text.errors

final case class BillTextProcessingFailed(
  billId: String,
  detail: String,
) extends Exception(s"Failed to process bill text for $billId: $detail")
