package repcheck.ingestion.bills.summary.errors

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class BillSummariesApiHttpErrorSpec extends AnyFlatSpec with Matchers {

  "BillSummariesApiHttpError" should "include status and body in the message" in {
    val err = BillSummariesApiHttpError(429, "Too Many Requests")
    val msg = err.getMessage
    val _   = msg should include("429")
    msg should include("Too Many Requests")
  }

  it should "expose statusCode via the HttpStatusError marker for the classifier" in {
    val err = BillSummariesApiHttpError(503, "Service Unavailable")
    err.statusCode shouldBe 503
  }

}
