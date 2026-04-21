package repcheck.ingestion.votes.xml

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * Pure-function unit tests for [[SenateVoteXmlClient]] helpers: vote-number validation boundaries and HTTP-status
 * unwrapping. Kept small and dependency-free so coverage is uncoupled from WireMock scheduling and so the boundary
 * rules are directly exercised without any HTTP surface.
 */
class SenateVoteXmlClientUnitSpec extends AnyFlatSpec with Matchers {

  "validateVoteNumber" should "accept 1 as the lower bound" in {
    SenateVoteXmlClient.validateVoteNumber(1) shouldBe Right(())
  }

  it should "accept typical single-digit vote numbers (9)" in {
    SenateVoteXmlClient.validateVoteNumber(9) shouldBe Right(())
  }

  it should "accept two-digit vote numbers (10, 99)" in {
    val _ = SenateVoteXmlClient.validateVoteNumber(10) shouldBe Right(())
    SenateVoteXmlClient.validateVoteNumber(99) shouldBe Right(())
  }

  it should "accept three-digit vote numbers (100, 999)" in {
    val _ = SenateVoteXmlClient.validateVoteNumber(100) shouldBe Right(())
    SenateVoteXmlClient.validateVoteNumber(999) shouldBe Right(())
  }

  it should "accept four-digit vote numbers (1000, 9999)" in {
    val _ = SenateVoteXmlClient.validateVoteNumber(1000) shouldBe Right(())
    SenateVoteXmlClient.validateVoteNumber(9999) shouldBe Right(())
  }

  it should "accept the maximum 5-digit vote number (99999)" in {
    SenateVoteXmlClient.validateVoteNumber(99999) shouldBe Right(())
  }

  it should "reject 0 as below the lower bound" in {
    SenateVoteXmlClient.validateVoteNumber(0).isLeft shouldBe true
  }

  it should "reject a negative vote number" in {
    val result = SenateVoteXmlClient.validateVoteNumber(-5)
    val _      = result.isLeft shouldBe true
    result.left.toOption.getOrElse("") should include(">= 1")
  }

  it should "reject 100000 as overflowing the 5-digit URL segment" in {
    val result = SenateVoteXmlClient.validateVoteNumber(100000)
    val _      = result.isLeft shouldBe true
    result.left.toOption.getOrElse("") should include("5 digits")
  }

  it should "reject very large vote numbers (e.g. Int.MaxValue)" in {
    SenateVoteXmlClient.validateVoteNumber(Int.MaxValue).isLeft shouldBe true
  }

  "unwrapHttpStatus" should "return None when no cause is supplied" in {
    SenateVoteXmlClient.unwrapHttpStatus(None) shouldBe None
  }

  it should "return None for a plain RuntimeException chain" in {
    val top   = new RuntimeException("top", new RuntimeException("middle"))
    val probe = SenateVoteXmlClient.unwrapHttpStatus(Some(top))
    probe shouldBe None
  }

  it should "extract the status code from a direct UnexpectedStatus" in {
    val us = org.http4s.client.UnexpectedStatus(
      org.http4s.Status.NotFound,
      org.http4s.Method.GET,
      org.http4s.Uri.unsafeFromString("http://example"),
    )
    SenateVoteXmlClient.unwrapHttpStatus(Some(us)) shouldBe Some(404)
  }

  it should "extract the status code from a nested UnexpectedStatus cause chain" in {
    val us = org.http4s.client.UnexpectedStatus(
      org.http4s.Status.ServiceUnavailable,
      org.http4s.Method.GET,
      org.http4s.Uri.unsafeFromString("http://example"),
    )
    val wrapped = new RuntimeException("decode failed", us)
    SenateVoteXmlClient.unwrapHttpStatus(Some(wrapped)) shouldBe Some(503)
  }

}
