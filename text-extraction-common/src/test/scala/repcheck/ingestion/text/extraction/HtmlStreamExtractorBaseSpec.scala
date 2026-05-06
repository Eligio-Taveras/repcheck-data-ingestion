package repcheck.ingestion.text.extraction

import java.nio.charset.StandardCharsets
import java.util.concurrent.LinkedBlockingQueue

import cats.effect.IO
import cats.effect.unsafe.implicits.global

import fs2.Stream

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.xml.sax.helpers.AttributesImpl

/**
 * Specs for [[HtmlStreamExtractorBase]] focusing on the subclassing hooks and the SAX-handler suppression branches that
 * the default [[BillsHtmlStreamExtractor]] doesn't exercise.
 */
class HtmlStreamExtractorBaseSpec extends AnyFlatSpec with Matchers {

  private def bytesOf(content: String): Stream[IO, Byte] =
    Stream.emits(content.getBytes(StandardCharsets.UTF_8))

  private def joined(extractor: HtmlStreamExtractorBase[IO], html: String): String =
    extractor
      .extract(bytesOf(html))
      .compile
      .toList
      .unsafeRunSync()
      .mkString
      .replaceAll("\\s+", " ")
      .trim

  "default shouldKeepNode" should "be exercised by the default base instance" in {
    val extractor = new HtmlStreamExtractorBase[IO] {}
    val html      = "<html><body><p>keep me</p></body></html>"
    joined(extractor, html) should include("keep me")
  }

  "default transformText" should "leave content unchanged" in {
    val extractor = new HtmlStreamExtractorBase[IO] {}
    val html      = "<html><body><p>raw text</p></body></html>"
    joined(extractor, html) should include("raw text")
  }

  "shouldKeepNode override" should "suppress matching subtrees and resume after end-tag" in {
    // Suppresses the entire <skip> subtree (including nested elements) and resumes after </skip>.
    val extractor = new HtmlStreamExtractorBase[IO] {
      override protected def shouldKeepNode(name: String): Boolean = name != "skip"
    }
    val html =
      "<html><body><p>before</p><skip><span>nested suppressed</span></skip><p>after</p></body></html>"
    val result = joined(extractor, html)
    val _      = result should include("before")
    val _      = result should include("after")
    val _      = result should not include "nested suppressed"
    result should not include "skip"
  }

  it should "suppress nested matching subtrees correctly" in {
    // Two-level suppression: outer <skip> suppresses; nested <skip> increments suppressedDepth again.
    // Both inner content and the second inner <skip> stay suppressed.
    val extractor = new HtmlStreamExtractorBase[IO] {
      override protected def shouldKeepNode(name: String): Boolean = name != "skip"
    }
    val html =
      "<html><body><skip>outer<skip>inner</skip>after-inner</skip><p>visible</p></body></html>"
    val result = joined(extractor, html)
    val _      = result should include("visible")
    val _      = result should not include "outer"
    val _      = result should not include "inner"
    result should not include "after-inner"
  }

  "transformText override" should "rewrite text fragments before whitespace collapse" in {
    val extractor = new HtmlStreamExtractorBase[IO] {
      override protected def transformText(text: String): String = text.replace("PAGE_NUM", "")
    }
    val html   = "<html><body><p>before PAGE_NUM after</p></body></html>"
    val result = joined(extractor, html)
    val _      = result should include("before")
    val _      = result should include("after")
    result should not include "PAGE_NUM"
  }

  it should "drop fragments whose transformed text becomes empty after collapse" in {
    // transformText returns whitespace-only for content matching the pattern; collapseWhitespace turns it into a
    // single space, then the body-collapse-then-empty check filters it out at the queue boundary.
    val extractor = new HtmlStreamExtractorBase[IO] {
      override protected def transformText(text: String): String =
        if (text.contains("DROP")) "" else text
    }
    val html   = "<html><body><p>keep1</p><p>DROP_ME</p><p>keep2</p></body></html>"
    val result = joined(extractor, html)
    val _      = result should include("keep1")
    val _      = result should include("keep2")
    result should not include "DROP_ME"
  }

  // -- Direct SAX-handler tests --------------------------------------------------------------------------------------
  // The SAX handler exposes ContentHandler callbacks that TagSoup never emits for typical Congress.gov HTML
  // (ignorableWhitespace, processingInstruction, skippedEntity) and defensive depth-underflow paths in endElement
  // that only trigger on malformed input. These tests drive the handler directly to lock in that behavior.

  "TextEmittingHandler" should "no-op on ignorableWhitespace, processingInstruction, skippedEntity" in {
    val extractor = new HtmlStreamExtractorBase[IO] {}
    val queue     = new LinkedBlockingQueue[Option[String]](16)
    val handler   = extractor.newHandler(queue)

    handler.ignorableWhitespace("   ".toCharArray, 0, 3)
    handler.processingInstruction("xml-stylesheet", "href='x.css'")
    handler.skippedEntity("nbsp")

    queue.size() shouldBe 0
  }

  it should "no-op on the other ContentHandler lifecycle callbacks" in {
    // Locator/document/prefix-mapping callbacks are part of the SAX contract but produce no output for our use case.
    val extractor = new HtmlStreamExtractorBase[IO] {}
    val queue     = new LinkedBlockingQueue[Option[String]](16)
    val handler   = extractor.newHandler(queue)

    handler.setDocumentLocator(new org.xml.sax.helpers.LocatorImpl)
    handler.startDocument()
    handler.endDocument()
    handler.startPrefixMapping("p", "urn:p")
    handler.endPrefixMapping("p")

    queue.size() shouldBe 0
  }

  it should "guard against endElement underflow on malformed sequences (defensive depth-zero paths)" in {
    // The endElement decrement-only-if-positive guards exist for malformed event sequences where an end
    // tag arrives without a prior start tag at the corresponding depth. TagSoup typically rebalances such
    // input so these guards aren't naturally exercised — drive them directly via the SAX API.
    val extractor = new HtmlStreamExtractorBase[IO] {}
    val queue     = new LinkedBlockingQueue[Option[String]](16)
    val handler   = extractor.newHandler(queue)

    val noAtts = new AttributesImpl
    // Invoke end-tags for body/script/style without matching start-tags. The defensive guard branches
    // (depth==0 → skip decrement) execute.
    handler.endElement("", "body", "body")
    handler.endElement("", "script", "script")
    handler.endElement("", "style", "style")
    // Drive a normal body cycle afterward to confirm the handler is still usable.
    handler.startElement("", "body", "body", noAtts)
    handler.characters("hello".toCharArray, 0, 5)
    handler.endElement("", "body", "body")

    val drained =
      Iterator.continually(queue.poll()).takeWhile(_ != null).flatten.toList
    drained should contain("hello")
  }

  it should "fall back to qName when localName is empty in elementName" in {
    // TagSoup always supplies localName for HTML elements, so the qName fallback in elementName is a
    // defensive branch for SAX implementations that omit localName. Drive the handler with an empty
    // localName + a populated qName to exercise that branch.
    val extractor = new HtmlStreamExtractorBase[IO] {}
    val queue     = new LinkedBlockingQueue[Option[String]](16)
    val handler   = extractor.newHandler(queue)

    val noAtts = new AttributesImpl
    handler.startElement("", "", "body", noAtts) // empty localName, qName="body"
    handler.characters("via-qname".toCharArray, 0, 9)
    handler.endElement("", "", "body")

    val drained =
      Iterator.continually(queue.poll()).takeWhile(_ != null).flatten.toList
    drained should contain("via-qname")
  }

  it should "fall back to empty string when both localName and qName are empty" in {
    // The other fallback in elementName: localName empty AND qName empty → return "". The element name
    // doesn't match body/script/style/suppressed, so the handler treats it as a no-op outside body and
    // emits nothing.
    val extractor = new HtmlStreamExtractorBase[IO] {}
    val queue     = new LinkedBlockingQueue[Option[String]](16)
    val handler   = extractor.newHandler(queue)

    val noAtts = new AttributesImpl
    handler.startElement("", "", "", noAtts)
    handler.endElement("", "", "")

    queue.size() shouldBe 0
  }

}
