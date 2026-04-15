package repcheck.members.common.errors

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class MemberUpsertFailedSpec extends AnyFlatSpec with Matchers {

  "MemberUpsertFailed" should "format message with bioguideId and detail" in {
    val error = MemberUpsertFailed("A000001", "connection refused")
    error.getMessage shouldBe "Failed to upsert member A000001: connection refused"
  }

  it should "expose bioguideId field" in {
    val error = MemberUpsertFailed("B000002", "timeout")
    error.bioguideId shouldBe "B000002"
  }

  it should "expose detail field" in {
    val error = MemberUpsertFailed("C000003", "duplicate key")
    error.detail shouldBe "duplicate key"
  }

  it should "default cause to None" in {
    val error = MemberUpsertFailed("D000004", "unknown")
    error.cause shouldBe None
  }

  it should "chain cause when provided" in {
    val rootCause = new RuntimeException("root")
    val error     = MemberUpsertFailed("E000005", "wrapped", Some(rootCause))
    val _         = error.cause shouldBe Some(rootCause)
    error.getCause shouldBe rootCause
  }

  it should "be an instance of Exception" in {
    val error = MemberUpsertFailed("F000006", "test")
    error shouldBe a[Exception]
  }

}
