package repcheck.ingestion.amendments.text.errors

final case class AmendmentNotFoundForText(
  amendmentNaturalKey: String
) extends Exception(s"Amendment not found in database for natural key: $amendmentNaturalKey")
