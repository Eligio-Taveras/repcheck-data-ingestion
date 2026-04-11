package com.repcheck.bills.text.errors

final case class TextDownloadFailed(
  textUrl: String,
  textFormat: String,
  detail: String,
  cause: Throwable,
) extends Exception(
      s"Failed to download bill text from $textUrl (format=$textFormat): $detail",
      cause,
    )
