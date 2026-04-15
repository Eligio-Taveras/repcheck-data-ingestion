package repcheck.members.lismapping.errors

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class LisMappingFetchFailedSpec extends AnyFlatSpec with Matchers {

  "LisMappingFetchFailed" should "format message with detail" in {
    val error = LisMappingFetchFailed("connection refused")
    error.getMessage shouldBe "Failed to fetch LIS mapping: connection refused"
  }

  it should "expose detail field" in {
    val error = LisMappingFetchFailed("timeout")
    error.detail shouldBe "timeout"
  }

  it should "default cause to None" in {
    val error = LisMappingFetchFailed("unknown")
    error.cause shouldBe None
  }

  it should "chain cause when provided" in {
    val rootCause = new RuntimeException("root")
    val error     = LisMappingFetchFailed("wrapped", Some(rootCause))
    val _         = error.cause shouldBe Some(rootCause)
    error.getCause shouldBe rootCause
  }

  it should "be an instance of Exception" in {
    val error = LisMappingFetchFailed("test")
    error shouldBe a[Exception]
  }

}
