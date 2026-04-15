package repcheck.ingestion.bills.text.errors

final case class BillNotFoundForText(
  billNaturalKey: String
) extends Exception(s"Bill not found in database for natural key: $billNaturalKey")
