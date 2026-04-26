package repcheck.ingestion.bills.summary.errors

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class BillSummaryFetchFailedSpec extends AnyFlatSpec with Matchers {

  "BillSummaryFetchFailed" should "include endpoint, status, and detail in the message" in {
    val cause = new RuntimeException("boom")
    val err   = BillSummaryFetchFailed("https://api.congress.gov/v3/summaries/119", 503, "service unavailable", cause)

    val msg = err.getMessage
    val _   = msg should include("https://api.congress.gov/v3/summaries/119")
    val _   = msg should include("503")
    msg should include("service unavailable")
  }

  it should "preserve the cause for chained-exception inspection" in {
    val cause = new IllegalStateException("nested")
    val err   = BillSummaryFetchFailed("http://x", 0, "no body", cause)
    err.getCause shouldBe cause
  }

}
