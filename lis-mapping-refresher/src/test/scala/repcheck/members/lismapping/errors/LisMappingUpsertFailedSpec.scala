package repcheck.members.lismapping.errors

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class LisMappingUpsertFailedSpec extends AnyFlatSpec with Matchers {

  "LisMappingUpsertFailed" should "format message with lisId and detail" in {
    val error = LisMappingUpsertFailed("S001", "connection refused")
    error.getMessage shouldBe "Failed to upsert LIS mapping for S001: connection refused"
  }

  it should "expose lisId field" in {
    val error = LisMappingUpsertFailed("S002", "timeout")
    error.lisId shouldBe "S002"
  }

  it should "expose detail field" in {
    val error = LisMappingUpsertFailed("S003", "duplicate key")
    error.detail shouldBe "duplicate key"
  }

  it should "default cause to None" in {
    val error = LisMappingUpsertFailed("S004", "unknown")
    error.cause shouldBe None
  }

  it should "chain cause when provided" in {
    val rootCause = new RuntimeException("root")
    val error     = LisMappingUpsertFailed("S005", "wrapped", Some(rootCause))
    val _         = error.cause shouldBe Some(rootCause)
    error.getCause shouldBe rootCause
  }

  it should "be an instance of Exception" in {
    val error = LisMappingUpsertFailed("S006", "test")
    error shouldBe a[Exception]
  }

}
