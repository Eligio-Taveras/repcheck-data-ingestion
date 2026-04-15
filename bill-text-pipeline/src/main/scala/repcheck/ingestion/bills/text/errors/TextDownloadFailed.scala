package repcheck.ingestion.bills.text.errors

final case class TextDownloadFailed(
  textUrl: String,
  textFormat: String,
  detail: String,
) extends Exception(
      s"Failed to download bill text from $textUrl (format=$textFormat): $detail"
    )
