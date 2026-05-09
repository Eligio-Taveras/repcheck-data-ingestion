package repcheck.ingestion.amendments.text.errors

final case class InvalidAmendmentTextUrl(
  textUrl: String,
  detail: String,
) extends Exception(s"Invalid amendment text URL '$textUrl': $detail")
