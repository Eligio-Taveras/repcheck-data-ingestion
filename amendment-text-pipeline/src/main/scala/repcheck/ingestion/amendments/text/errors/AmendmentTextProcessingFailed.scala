package repcheck.ingestion.amendments.text.errors

final case class AmendmentTextProcessingFailed(
  amendmentNaturalKey: String,
  detail: String,
) extends Exception(s"Failed to process amendment text for $amendmentNaturalKey: $detail")
