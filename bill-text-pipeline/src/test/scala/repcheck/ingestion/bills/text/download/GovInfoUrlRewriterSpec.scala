package repcheck.ingestion.bills.text.download

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * Spec for the [[GovInfoUrlRewriter]] pure URL transform.
 *
 * The Congress.gov-style input URLs covered here are sampled directly from the bill-text-pipeline production logs
 * (BILLS-119hr2384rfs.htm, BILLS-118sjres89pcs.xml, etc.) so the regex stays grounded in real shapes.
 */
class GovInfoUrlRewriterSpec extends AnyFlatSpec with Matchers {

  "parseCongressGovBillUrl" should "extract package id and htm format from a www.congress.gov HTM URL" in {
    GovInfoUrlRewriter
      .parseCongressGovBillUrl("https://www.congress.gov/119/bills/hr2384/BILLS-119hr2384rfs.htm") shouldBe
      Some(("BILLS-119hr2384rfs", "htm"))
  }

  it should "extract package id and xml format" in {
    GovInfoUrlRewriter
      .parseCongressGovBillUrl("https://www.congress.gov/118/bills/sjres89/BILLS-118sjres89pcs.xml") shouldBe
      Some(("BILLS-118sjres89pcs", "xml"))
  }

  it should "extract package id and pdf format" in {
    GovInfoUrlRewriter
      .parseCongressGovBillUrl("https://www.congress.gov/117/bills/hr3076/BILLS-117hr3076enr.pdf") shouldBe
      Some(("BILLS-117hr3076enr", "pdf"))
  }

  it should "match http (no s) congress.gov URLs" in {
    GovInfoUrlRewriter
      .parseCongressGovBillUrl("http://www.congress.gov/119/bills/hr1/BILLS-119hr1ih.htm") shouldBe
      Some(("BILLS-119hr1ih", "htm"))
  }

  it should "match a bare congress.gov host (no www) too" in {
    GovInfoUrlRewriter
      .parseCongressGovBillUrl("https://congress.gov/119/bills/hr1/BILLS-119hr1ih.htm") shouldBe
      Some(("BILLS-119hr1ih", "htm"))
  }

  it should "extract from URLs with various bill type prefixes (hr, hres, s, sjres, etc.)" in {
    val cases = List(
      "https://www.congress.gov/119/bills/hres217/BILLS-119hres217ih.htm"   -> ("BILLS-119hres217ih", "htm"),
      "https://www.congress.gov/119/bills/s165/BILLS-119s165is.htm"         -> ("BILLS-119s165is", "htm"),
      "https://www.congress.gov/118/bills/sjres12/BILLS-118sjres12is.htm"   -> ("BILLS-118sjres12is", "htm"),
      "https://www.congress.gov/117/bills/hjres19/BILLS-117hjres19enr.htm"  -> ("BILLS-117hjres19enr", "htm"),
      "https://www.congress.gov/118/bills/sconres5/BILLS-118sconres5is.htm" -> ("BILLS-118sconres5is", "htm"),
    )
    cases.foreach {
      case (url, expected) =>
        val _ = GovInfoUrlRewriter.parseCongressGovBillUrl(url) shouldBe Some(expected)
    }
  }

  it should "return None for a non-congress.gov host" in {
    val _ =
      GovInfoUrlRewriter.parseCongressGovBillUrl("https://example.com/119/bills/hr1/BILLS-119hr1ih.htm") shouldBe None
    GovInfoUrlRewriter.parseCongressGovBillUrl("https://api.govinfo.gov/packages/BILLS-119hr1ih/htm") shouldBe None
  }

  it should "return None when the path doesn't start with /{congress}/bills/" in {
    GovInfoUrlRewriter.parseCongressGovBillUrl("https://www.congress.gov/help/using-data-offsite") shouldBe None
  }

  it should "return None for unrecognized format extensions (.html, .txt, .json, etc.)" in {
    val _ = GovInfoUrlRewriter
      .parseCongressGovBillUrl("https://www.congress.gov/119/bills/hr1/BILLS-119hr1ih.html") shouldBe None
    val _ = GovInfoUrlRewriter
      .parseCongressGovBillUrl("https://www.congress.gov/119/bills/hr1/BILLS-119hr1ih.txt") shouldBe None
    GovInfoUrlRewriter
      .parseCongressGovBillUrl("https://www.congress.gov/119/bills/hr1/BILLS-119hr1ih.json") shouldBe None
  }

  it should "return None for a non-BILLS file segment" in {
    GovInfoUrlRewriter
      .parseCongressGovBillUrl("https://www.congress.gov/119/bills/hr1/Other-119hr1ih.htm") shouldBe None
  }

  it should "return None for an empty string or garbage input" in {
    val _ = GovInfoUrlRewriter.parseCongressGovBillUrl("") shouldBe None
    GovInfoUrlRewriter.parseCongressGovBillUrl("not a url") shouldBe None
  }

  it should "tolerate trailing query strings (e.g., ?utm_source=...)" in {
    // Defensive: in practice Congress.gov doesn't append query strings to format URLs, but we shouldn't break
    // the rewrite if one appears (e.g., from a logged copy-paste).
    GovInfoUrlRewriter
      .parseCongressGovBillUrl("https://www.congress.gov/119/bills/hr1/BILLS-119hr1ih.htm?utm_source=test") shouldBe
      Some(("BILLS-119hr1ih", "htm"))
  }

  "toGovInfoUrl" should "construct the standard package URL" in {
    GovInfoUrlRewriter.toGovInfoUrl("BILLS-119hr2384rfs", "htm", "https://api.govinfo.gov") shouldBe
      "https://api.govinfo.gov/packages/BILLS-119hr2384rfs/htm"
  }

  it should "strip a trailing slash on the base URL to avoid '//packages'" in {
    GovInfoUrlRewriter.toGovInfoUrl("BILLS-119hr1ih", "htm", "https://api.govinfo.gov/") shouldBe
      "https://api.govinfo.gov/packages/BILLS-119hr1ih/htm"
  }

  it should "support a non-default base URL (for tests pointing at WireMock)" in {
    GovInfoUrlRewriter.toGovInfoUrl("BILLS-119hr1ih", "xml", "http://127.0.0.1:9876") shouldBe
      "http://127.0.0.1:9876/packages/BILLS-119hr1ih/xml"
  }

}
