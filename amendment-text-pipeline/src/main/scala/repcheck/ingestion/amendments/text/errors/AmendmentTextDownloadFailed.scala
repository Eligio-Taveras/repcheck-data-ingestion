package repcheck.ingestion.amendments.text.errors

final case class AmendmentTextDownloadFailed(
  textUrl: String,
  formatType: String,
  detail: String,
) extends Exception(
      s"Failed to download amendment text from $textUrl (format=$formatType): $detail"
    )
