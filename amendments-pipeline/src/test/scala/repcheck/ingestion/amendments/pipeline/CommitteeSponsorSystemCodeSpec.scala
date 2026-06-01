package repcheck.ingestion.amendments.pipeline

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class CommitteeSponsorSystemCodeSpec extends AnyFlatSpec with Matchers {

  "fromUrl" should "extract the systemCode from a House committee sponsor URL" in {
    CommitteeSponsorSystemCode.fromUrl(
      "https://api.congress.gov/v3/committee/house/hsru00?format=json"
    ) shouldBe Some("hsru00")
  }

  it should "extract the systemCode from a Senate committee sponsor URL" in {
    CommitteeSponsorSystemCode.fromUrl(
      "https://api.congress.gov/v3/committee/senate/ssra00?format=json"
    ) shouldBe Some("ssra00")
  }

  it should "extract the systemCode when there is no query string" in {
    CommitteeSponsorSystemCode.fromUrl(
      "https://api.congress.gov/v3/committee/house/hswm00"
    ) shouldBe Some("hswm00")
  }

  it should "return None for a URL with no committee path segment" in {
    CommitteeSponsorSystemCode.fromUrl(
      "https://api.congress.gov/v3/amendment/117/samdt/100"
    ) shouldBe None
  }

  it should "return None for an empty string" in {
    CommitteeSponsorSystemCode.fromUrl("") shouldBe None
  }

}
