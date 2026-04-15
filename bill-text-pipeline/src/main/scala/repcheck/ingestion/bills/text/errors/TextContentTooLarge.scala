package repcheck.ingestion.bills.text.errors

final case class TextContentTooLarge(
  textUrl: String,
  actualBytes: Long,
  maxBytes: Long,
) extends Exception(s"Bill text from $textUrl is $actualBytes bytes, exceeding maximum $maxBytes bytes")
