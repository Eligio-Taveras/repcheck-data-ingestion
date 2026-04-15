package repcheck.ingestion.bills.text.errors

final case class InvalidTextUrl(
  textUrl: String,
  detail: String,
) extends Exception(s"Invalid bill text URL '$textUrl': $detail")
