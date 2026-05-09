package repcheck.ingestion.amendments.text.download

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * Spec for the [[CrecGovInfoUrlRewriter]] pure URL transform. Exercises every shape called out in §7.6 "Acceptance
 * Criteria" so the regex stays grounded in the URL patterns observed in production logs.
 */
class CrecGovInfoUrlRewriterSpec extends AnyFlatSpec with Matchers {

  "parseCongressGovCrecUrl" should "extract the canonical (packageId, granuleId, htm) tuple" in {
    CrecGovInfoUrlRewriter
      .parseCongressGovCrecUrl(
        "https://www.congress.gov/117/crec/2021/08/01/167/136/modified/CREC-2021-08-01-pt1-PgS5255.htm"
      ) shouldBe Some(("CREC-2021-08-01", "CREC-2021-08-01-pt1-PgS5255", "htm"))
  }

  it should "handle the non-modified path variant" in {
    CrecGovInfoUrlRewriter
      .parseCongressGovCrecUrl(
        "https://www.congress.gov/117/crec/2021/08/01/167/136/CREC-2021-08-01-pt1-PgS5255.pdf"
      ) shouldBe Some(("CREC-2021-08-01", "CREC-2021-08-01-pt1-PgS5255", "pdf"))
  }

  it should "handle dashed page suffixes like PgS1044-4" in {
    CrecGovInfoUrlRewriter
      .parseCongressGovCrecUrl(
        "https://www.congress.gov/119/crec/2025/02/19/171/33/modified/CREC-2025-02-19-pt1-PgS1044-4.htm"
      ) shouldBe Some(("CREC-2025-02-19", "CREC-2025-02-19-pt1-PgS1044-4", "htm"))
  }

  it should "match http (no s) URLs" in {
    CrecGovInfoUrlRewriter
      .parseCongressGovCrecUrl(
        "http://www.congress.gov/117/crec/2021/08/01/167/136/CREC-2021-08-01-pt1-PgS5255.htm"
      ) shouldBe Some(("CREC-2021-08-01", "CREC-2021-08-01-pt1-PgS5255", "htm"))
  }

  it should "match the bare congress.gov host (no www)" in {
    CrecGovInfoUrlRewriter
      .parseCongressGovCrecUrl(
        "https://congress.gov/117/crec/2021/08/01/167/136/CREC-2021-08-01-pt1-PgS5255.htm"
      ) shouldBe Some(("CREC-2021-08-01", "CREC-2021-08-01-pt1-PgS5255", "htm"))
  }

  it should "tolerate a trailing query string (defensive — strips ?api_key=leak)" in {
    CrecGovInfoUrlRewriter
      .parseCongressGovCrecUrl(
        "https://www.congress.gov/119/crec/2025/02/19/171/33/modified/CREC-2025-02-19-pt1-PgS1044-4.htm?api_key=leak"
      ) shouldBe Some(("CREC-2025-02-19", "CREC-2025-02-19-pt1-PgS1044-4", "htm"))
  }

  it should "return None for non-CREC URLs (BILLS-, etc.)" in {
    val _ = CrecGovInfoUrlRewriter
      .parseCongressGovCrecUrl("https://www.congress.gov/119/bills/hr1/BILLS-119hr1ih.htm") shouldBe None
    CrecGovInfoUrlRewriter.parseCongressGovCrecUrl("https://api.govinfo.gov/packages/CREC-x/htm") shouldBe None
  }

  it should "return None for unrecognized format extensions" in {
    val _ = CrecGovInfoUrlRewriter
      .parseCongressGovCrecUrl(
        "https://www.congress.gov/117/crec/2021/08/01/167/136/CREC-2021-08-01-pt1-PgS5255.html"
      ) shouldBe None
    CrecGovInfoUrlRewriter
      .parseCongressGovCrecUrl(
        "https://www.congress.gov/117/crec/2021/08/01/167/136/CREC-2021-08-01-pt1-PgS5255.txt"
      ) shouldBe None
  }

  it should "return None for empty string or garbage input" in {
    val _ = CrecGovInfoUrlRewriter.parseCongressGovCrecUrl("") shouldBe None
    CrecGovInfoUrlRewriter.parseCongressGovCrecUrl("not a url") shouldBe None
  }

  it should "return None for adversarial paths containing '..'" in {
    val _ = CrecGovInfoUrlRewriter
      .parseCongressGovCrecUrl(
        "https://www.congress.gov/117/crec/2021/08/01/../136/CREC-2021-08-01-pt1-PgS5255.htm"
      ) shouldBe None
    CrecGovInfoUrlRewriter
      .parseCongressGovCrecUrl(
        "https://www.congress.gov/117/crec/2021/08/01/167/136/modified/CREC-2021-08-01-pt1-PgS5255.htm/.."
      ) shouldBe None
  }

  "toGovInfoUrl" should "construct the granule-scoped api.govinfo.gov path" in {
    CrecGovInfoUrlRewriter.toGovInfoUrl(
      "CREC-2021-08-01",
      "CREC-2021-08-01-pt1-PgS5255",
      "htm",
      "https://api.govinfo.gov",
    ) shouldBe "https://api.govinfo.gov/packages/CREC-2021-08-01/granules/CREC-2021-08-01-pt1-PgS5255/htm"
  }

  it should "strip a trailing slash on the base URL to avoid '//packages'" in {
    CrecGovInfoUrlRewriter.toGovInfoUrl(
      "CREC-2021-08-01",
      "CREC-2021-08-01-pt1-PgS5255",
      "pdf",
      "https://api.govinfo.gov/",
    ) shouldBe "https://api.govinfo.gov/packages/CREC-2021-08-01/granules/CREC-2021-08-01-pt1-PgS5255/pdf"
  }

  it should "support a non-default base URL (for tests pointing at WireMock)" in {
    CrecGovInfoUrlRewriter.toGovInfoUrl(
      "CREC-2021-08-01",
      "CREC-2021-08-01-pt1-PgS5255",
      "htm",
      "http://127.0.0.1:9876",
    ) shouldBe "http://127.0.0.1:9876/packages/CREC-2021-08-01/granules/CREC-2021-08-01-pt1-PgS5255/htm"
  }

}
