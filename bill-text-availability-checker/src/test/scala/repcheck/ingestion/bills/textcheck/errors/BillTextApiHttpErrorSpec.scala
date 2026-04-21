package repcheck.ingestion.bills.textcheck.errors

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class BillTextApiHttpErrorSpec extends AnyFlatSpec with Matchers {

  "BillTextApiHttpError" should "format message with status code and body" in {
    val err = BillTextApiHttpError(429, "Rate limited")
    err.getMessage shouldBe "Congress.gov bill-text API returned HTTP 429: Rate limited"
  }

  it should "expose statusCode field" in {
    BillTextApiHttpError(500, "Internal error").statusCode shouldBe 500
  }

  it should "expose body field" in {
    BillTextApiHttpError(503, "Service unavailable").body shouldBe "Service unavailable"
  }

  it should "be a Throwable subclass" in {
    BillTextApiHttpError(404, "not found") shouldBe a[Exception]
  }

}
