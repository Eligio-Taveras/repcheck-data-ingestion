package repcheck.ingestion.members.profile.errors

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class MemberFetchFailedSpec extends AnyFlatSpec with Matchers {

  "MemberFetchFailed" should "format message with bioguideId when present" in {
    val error = MemberFetchFailed(Some("A000001"), "connection refused")
    error.getMessage shouldBe "Failed to fetch member A000001: connection refused"
  }

  it should "format message with <unknown> when bioguideId is absent" in {
    val error = MemberFetchFailed(None, "timeout")
    error.getMessage shouldBe "Failed to fetch member <unknown>: timeout"
  }

  it should "expose bioguideId field" in {
    val error = MemberFetchFailed(Some("B000002"), "not found")
    error.bioguideId shouldBe Some("B000002")
  }

  it should "expose detail field" in {
    val error = MemberFetchFailed(Some("C000003"), "rate limited")
    error.detail shouldBe "rate limited"
  }

  it should "default cause to None" in {
    val error = MemberFetchFailed(Some("D000004"), "unknown")
    error.cause shouldBe None
  }

  it should "chain cause when provided" in {
    val rootCause = new RuntimeException("root")
    val error     = MemberFetchFailed(Some("E000005"), "wrapped", Some(rootCause))
    val _         = error.cause shouldBe Some(rootCause)
    error.getCause shouldBe rootCause
  }

  it should "be an instance of Exception" in {
    val error = MemberFetchFailed(Some("F000006"), "test")
    error shouldBe a[Exception]
  }

}
