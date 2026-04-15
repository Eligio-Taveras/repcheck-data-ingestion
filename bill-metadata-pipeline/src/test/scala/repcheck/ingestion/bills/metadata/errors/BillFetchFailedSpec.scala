package repcheck.ingestion.bills.metadata.errors

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class BillFetchFailedSpec extends AnyFlatSpec with Matchers {

  private val cause = new RuntimeException("connection reset")

  "BillFetchFailed" should "format message with endpoint, status code, and detail" in {
    val err = BillFetchFailed("/bill/118/hr/1", 429, "rate limited", cause)
    err.getMessage shouldBe "Failed to fetch bills from /bill/118/hr/1 (HTTP 429): rate limited"
  }

  it should "preserve the cause" in {
    BillFetchFailed("/bill", 500, "server error", cause).getCause shouldBe cause
  }

  it should "expose endpoint field" in {
    BillFetchFailed("/bill/118/hr/1", 404, "not found", cause).endpoint shouldBe "/bill/118/hr/1"
  }

  it should "expose statusCode field" in {
    BillFetchFailed("/bill/118/hr/1", 404, "not found", cause).statusCode shouldBe 404
  }

  it should "expose detail field" in {
    BillFetchFailed("/bill/118/hr/1", 404, "not found", cause).detail shouldBe "not found"
  }

  it should "be a Throwable subclass" in {
    BillFetchFailed("/bill", 0, "test", cause) shouldBe a[Exception]
  }

}
