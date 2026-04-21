package repcheck.ingestion.votes.errors

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class XmlParseFailedSpec extends AnyFlatSpec with Matchers {

  "XmlParseFailed" should "include the detail in its message" in {
    val err = XmlParseFailed("missing <congress>", Some("<roll_call_vote/>"))
    err.getMessage should include("missing <congress>")
  }

  it should "permit a None rawFragment when no source context is available" in {
    val err = XmlParseFailed("bad header", None)
    err.rawFragment shouldBe None
  }

  it should "expose rawFragment when supplied" in {
    val err = XmlParseFailed("odd body", Some("<xml>x</xml>"))
    err.rawFragment shouldBe Some("<xml>x</xml>")
  }

}
