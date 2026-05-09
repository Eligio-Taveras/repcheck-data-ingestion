package repcheck.ingestion.amendments.text.extraction

import cats.effect.IO
import cats.effect.unsafe.implicits.global

import fs2.Stream

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class CrecHtmlExtractorSpec extends AnyFlatSpec with Matchers {

  private def extractFromHtml(html: String): String = {
    val bytes = Stream.emits(html.getBytes("UTF-8")).covary[IO]
    CrecHtmlExtractor.extract[IO](bytes).compile.toList.unsafeRunSync().mkString(" ").trim
  }

  "stripCrecNoise" should "drop inline page references like [[Page S5256]]" in {
    val raw = "Section 1. [[Page S5256]] Some text."
    CrecHtmlExtractor.stripCrecNoise(raw) should not include "[[Page"
  }

  it should "drop bracketed time-of-day annotations" in {
    val raw = "[Time: 10:30 a.m.] Mr. SCHUMER. I rise to ..."
    CrecHtmlExtractor.stripCrecNoise(raw) should not include "[Time:"
  }

  it should "drop standalone Pg S5255 page-number footers" in {
    CrecHtmlExtractor.stripCrecNoise("Some text Pg S5255 trailing") should not include "Pg S5255"
  }

  it should "preserve speaker tags and section markers" in {
    val raw = "Mr. SCHUMER. Section 1. (a) Whereas..."
    val out = CrecHtmlExtractor.stripCrecNoise(raw)
    val _   = out should include("Mr. SCHUMER")
    val _   = out should include("Section 1")
    out should include("(a)")
  }

  it should "preserve normal text after stripping noise" in {
    val raw = "[Time: 10:30 a.m.] Mr. SCHUMER. The text continues. [[Page S5256]] More text."
    val out = CrecHtmlExtractor.stripCrecNoise(raw)
    val _   = out should include("Mr. SCHUMER")
    val _   = out should include("The text continues")
    out should include("More text")
  }

  it should "be idempotent (stripping twice gives the same result)" in {
    val raw   = "[Time: x] Section 1. [[Page S1]] body Pg S2"
    val once  = CrecHtmlExtractor.stripCrecNoise(raw)
    val twice = CrecHtmlExtractor.stripCrecNoise(once)
    twice shouldBe once
  }

  "extract" should "extract body text from a CREC-shaped HTML document" in {
    val html = """
      |<html>
      |  <body>
      |    <div class="hd">CONGRESSIONAL RECORD - SENATE August 1, 2021</div>
      |    <pre>
      |Mr. SCHUMER. I rise to offer Amendment 2137. Section 1. The text reads as follows.
      |[[Page S5256]] Continuing.
      |    </pre>
      |  </body>
      |</html>
      |""".stripMargin
    val out = extractFromHtml(html)
    val _   = out should include("Mr. SCHUMER")
    val _   = out should include("Amendment 2137")
    out should not include "[[Page"
  }

  it should "decode HTML entities to literal characters via the underlying parser" in {
    val html = "<html><body><pre>Section 1 &mdash; first sentence. &amp; cont.</pre></body></html>"
    val out  = extractFromHtml(html)
    // The underlying TagSoup parser decodes named entities to their literal characters.
    val _ = out should include("—")
    out should include("&")
  }

  it should "return an empty string for an empty body" in {
    val html = "<html><body></body></html>"
    extractFromHtml(html) shouldBe ""
  }

  it should "skip <script> and <style> contents (inherited from base)" in {
    val html = "<html><body><script>var x = 1;</script>real text<style>a{color:red}</style></body></html>"
    val out  = extractFromHtml(html)
    val _    = out should include("real text")
    val _    = out should not include "var x"
    out should not include "color:red"
  }

}
