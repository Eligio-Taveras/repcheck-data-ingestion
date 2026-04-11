package com.repcheck.bills.text.errors

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class TextDownloadFailedSpec extends AnyFlatSpec with Matchers {

  "TextDownloadFailed" should "include URL in message" in {
    val ex =
      TextDownloadFailed("https://example.com/text", "Formatted Text", "timeout", new RuntimeException("timeout"))
    ex.getMessage should include("https://example.com/text")
  }

  it should "include format in message" in {
    val ex =
      TextDownloadFailed("https://example.com/text", "Formatted XML", "parse error", new RuntimeException("parse"))
    ex.getMessage should include("Formatted XML")
  }

  it should "include detail in message" in {
    val ex = TextDownloadFailed("https://example.com/text", "PDF", "file too large", new RuntimeException("size"))
    ex.getMessage should include("file too large")
  }

  it should "preserve the cause" in {
    val cause = new RuntimeException("connection refused")
    val ex    = TextDownloadFailed("https://example.com/text", "PDF", "network error", cause)
    ex.getCause shouldBe cause
  }

}
