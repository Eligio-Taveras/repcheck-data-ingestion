package repcheck.members.lismapping.errors

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class MemberPlaceholderFailedSpec extends AnyFlatSpec with Matchers {

  "MemberPlaceholderFailed" should "format message with bioguideId" in {
    val error = MemberPlaceholderFailed("A000001")
    error.getMessage shouldBe "Failed to create placeholder for member A000001"
  }

  it should "expose bioguideId field" in {
    val error = MemberPlaceholderFailed("B000002")
    error.bioguideId shouldBe "B000002"
  }

  it should "be an instance of Exception" in {
    val error = MemberPlaceholderFailed("C000003")
    error shouldBe a[Exception]
  }

}
