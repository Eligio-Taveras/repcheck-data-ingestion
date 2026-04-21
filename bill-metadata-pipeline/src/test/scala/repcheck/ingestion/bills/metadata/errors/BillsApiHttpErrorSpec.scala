package repcheck.ingestion.bills.metadata.errors

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class BillsApiHttpErrorSpec extends AnyFlatSpec with Matchers {

  "BillsApiHttpError" should "format message with status code and body" in {
    val err = BillsApiHttpError(429, "Rate limited")
    err.getMessage shouldBe "Congress.gov bills API returned HTTP 429: Rate limited"
  }

  it should "expose statusCode field" in {
    BillsApiHttpError(500, "Internal error").statusCode shouldBe 500
  }

  it should "expose body field" in {
    BillsApiHttpError(503, "Service unavailable").body shouldBe "Service unavailable"
  }

  it should "be a Throwable subclass" in {
    BillsApiHttpError(404, "not found") shouldBe a[Exception]
  }

}
