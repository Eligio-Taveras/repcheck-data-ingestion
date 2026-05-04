package repcheck.members.common.errors

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class InvalidBioguideIdSpec extends AnyFlatSpec with Matchers {

  "InvalidBioguideId" should "include the malformed value in the message" in {
    val error = InvalidBioguideId("bogus")
    error.getMessage should include("bogus")
  }

  it should "describe the expected format in the message" in {
    val error = InvalidBioguideId("bogus")
    val _     = error.getMessage should include("[A-Z]")
    error.getMessage should include("A000370")
  }

  it should "expose the malformed value field" in {
    val error = InvalidBioguideId("a000370")
    error.value shouldBe "a000370"
  }

  it should "be an instance of Exception" in {
    val error = InvalidBioguideId("")
    error shouldBe a[Exception]
  }

}
