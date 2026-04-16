package repcheck.ingestion.members.profile.errors

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class MemberProcessingFailedSpec extends AnyFlatSpec with Matchers {

  "MemberProcessingFailed" should "format message with bioguideId and detail" in {
    val error = MemberProcessingFailed("A000001", "post-commit re-read returned None")
    error.getMessage shouldBe "Failed to process member A000001: post-commit re-read returned None"
  }

  it should "expose bioguideId field" in {
    val error = MemberProcessingFailed("B000002", "unexpected state")
    error.bioguideId shouldBe "B000002"
  }

  it should "expose detail field" in {
    val error = MemberProcessingFailed("C000003", "DTO->DO conversion failed")
    error.detail shouldBe "DTO->DO conversion failed"
  }

  it should "default cause to None" in {
    val error = MemberProcessingFailed("D000004", "unknown")
    error.cause shouldBe None
  }

  it should "chain cause when provided" in {
    val rootCause = new RuntimeException("root")
    val error     = MemberProcessingFailed("E000005", "wrapped", Some(rootCause))
    val _         = error.cause shouldBe Some(rootCause)
    error.getCause shouldBe rootCause
  }

  it should "be an instance of Exception" in {
    val error = MemberProcessingFailed("F000006", "test")
    error shouldBe a[Exception]
  }

}
