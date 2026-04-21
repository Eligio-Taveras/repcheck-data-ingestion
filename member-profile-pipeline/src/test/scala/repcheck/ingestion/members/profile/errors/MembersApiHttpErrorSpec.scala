package repcheck.ingestion.members.profile.errors

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class MembersApiHttpErrorSpec extends AnyFlatSpec with Matchers {

  "MembersApiHttpError" should "format message with status code and body" in {
    val err = MembersApiHttpError(429, "Rate limited")
    err.getMessage shouldBe "Congress.gov members API returned HTTP 429: Rate limited"
  }

  it should "expose statusCode field" in {
    MembersApiHttpError(500, "Internal error").statusCode shouldBe 500
  }

  it should "expose body field" in {
    MembersApiHttpError(503, "Service unavailable").body shouldBe "Service unavailable"
  }

  it should "be a Throwable subclass" in {
    MembersApiHttpError(404, "not found") shouldBe a[Exception]
  }

}
