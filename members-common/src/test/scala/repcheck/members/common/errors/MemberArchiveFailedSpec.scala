package repcheck.members.common.errors

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class MemberArchiveFailedSpec extends AnyFlatSpec with Matchers {

  "MemberArchiveFailed" should "format message with bioguideId and detail" in {
    val error = MemberArchiveFailed("A000001", "table not found")
    error.getMessage shouldBe "Failed to archive member A000001: table not found"
  }

  it should "expose bioguideId field" in {
    val error = MemberArchiveFailed("B000002", "timeout")
    error.bioguideId shouldBe "B000002"
  }

  it should "expose detail field" in {
    val error = MemberArchiveFailed("C000003", "permission denied")
    error.detail shouldBe "permission denied"
  }

  it should "default cause to None" in {
    val error = MemberArchiveFailed("D000004", "unknown")
    error.cause shouldBe None
  }

  it should "chain cause when provided" in {
    val rootCause = new RuntimeException("root")
    val error     = MemberArchiveFailed("E000005", "wrapped", Some(rootCause))
    val _         = error.cause shouldBe Some(rootCause)
    error.getCause shouldBe rootCause
  }

  it should "be an instance of Exception" in {
    val error = MemberArchiveFailed("F000006", "test")
    error shouldBe a[Exception]
  }

}
