package com.repcheck.bills.text.errors

final case class TextDownloadTimedOut(
  textUrl: String,
  timeoutSeconds: Int,
) extends Exception(s"Download of bill text from $textUrl timed out after ${timeoutSeconds}s")
