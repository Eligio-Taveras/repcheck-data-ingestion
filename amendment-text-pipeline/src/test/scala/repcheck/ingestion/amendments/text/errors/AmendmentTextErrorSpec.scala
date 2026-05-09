package repcheck.ingestion.amendments.text.errors

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * Spec covering each of the flat unique exceptions thrown by the §7.6 amendment-text-pipeline. Mirrors the bill-side
 * per-error specs (e.g. `BillTextProcessingFailedSpec`); each exception gets its own brief test on the message format
 * so a future refactor of the message string surfaces here rather than in production logs.
 */
class AmendmentTextErrorSpec extends AnyFlatSpec with Matchers {

  "AmendmentTextDownloadFailed" should "include the URL, format, and detail in the message" in {
    val error = AmendmentTextDownloadFailed(
      textUrl = "https://example.com/x.htm",
      formatType = "HTML",
      detail = "HTTP 404 - amendment text not found",
    )
    val _ = error.getMessage should include("https://example.com/x.htm")
    val _ = error.getMessage should include("HTML")
    error.getMessage should include("HTTP 404 - amendment text not found")
  }

  "InvalidAmendmentTextUrl" should "carry both URL and detail" in {
    val error = InvalidAmendmentTextUrl(
      textUrl = "://broken",
      detail = "Invalid scheme",
    )
    val _ = error.getMessage should include("://broken")
    error.getMessage should include("Invalid scheme")
  }

  "AmendmentNotFoundForText" should "name the natural key in the message" in {
    val error = AmendmentNotFoundForText("117-SAMDT-2137")
    error.getMessage should include("117-SAMDT-2137")
  }

  "AmendmentTextProcessingFailed" should "carry the natural key + detail" in {
    val error = AmendmentTextProcessingFailed("117-SAMDT-2137", "embedder unexpectedly skipped")
    val _     = error.getMessage should include("117-SAMDT-2137")
    error.getMessage should include("embedder unexpectedly skipped")
  }

  "AmendmentTextDownloadHttpError" should "carry the status code and body" in {
    val error = AmendmentTextDownloadHttpError(429, "rate limit exceeded")
    val _     = error.statusCode shouldBe 429
    val _     = error.body shouldBe "rate limit exceeded"
    error.getMessage should include("429")
  }

}
