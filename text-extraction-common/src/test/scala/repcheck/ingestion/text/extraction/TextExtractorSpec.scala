package repcheck.ingestion.text.extraction

import java.io.ByteArrayOutputStream
import java.nio.charset.StandardCharsets

import cats.effect.IO
import cats.effect.unsafe.implicits.global

import fs2.Stream

import org.apache.pdfbox.pdmodel.common.PDRectangle
import org.apache.pdfbox.pdmodel.font.{PDType1Font, Standard14Fonts}
import org.apache.pdfbox.pdmodel.{PDDocument, PDPage, PDPageContentStream}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * Specs for the streaming [[TextExtractor]] dispatcher. Phase 3 of the bill-text-10mb plan: the dispatcher consumes a
 * `Stream[F, Byte]` directly, so HTML / XML / plain-text bytes flow from the HTTP socket through the parser without
 * ever touching disk. PDF is the one exception: it spools to a temp file inside `PdfStreamExtractor`.
 *
 * Tests construct in-memory byte streams from string fixtures (UTF-8 encoded) and consume the resulting `Stream[IO,
 * String]` via `compile.toList`. We don't assert exact fragment counts — those are parser-implementation details that
 * vary between StAX, TagSoup, and PDFBox, and asserting them would couple tests to internals.
 */
class TextExtractorSpec extends AnyFlatSpec with Matchers {

  private def bytesOf(content: String): Stream[IO, Byte] =
    Stream.emits(content.getBytes(StandardCharsets.UTF_8))

  private def pdfBytes(text: String): Stream[IO, Byte] = {
    val document = new PDDocument()
    val baos     = new ByteArrayOutputStream()
    try {
      val page = new PDPage(PDRectangle.LETTER)
      document.addPage(page)

      val font          = new PDType1Font(Standard14Fonts.FontName.HELVETICA)
      val contentStream = new PDPageContentStream(document, page)
      try {
        contentStream.beginText()
        contentStream.setFont(font, 12f)
        contentStream.newLineAtOffset(72, 720)
        contentStream.showText(text)
        contentStream.endText()
      } finally contentStream.close()

      document.save(baos)
    } finally document.close()
    Stream.emits(baos.toByteArray)
  }

  private def joined(bytes: Stream[IO, Byte], format: String): String =
    TextExtractor
      .extractStream[IO](bytes, format)
      .compile
      .toList
      .unsafeRunSync()
      .mkString
      .trim
      .replaceAll("\\s+", " ")

  "extractStream" should "extract <pre> contents from a 'Formatted Text' HTML body" in {
    val html =
      """<html>
        |  <body>
        |    <pre>SECTION 1. Title — first sentence.
        |Section 2. Second sentence.</pre>
        |  </body>
        |</html>""".stripMargin

    val result = joined(bytesOf(html), "Formatted Text")
    val _      = result should include("SECTION 1. Title")
    val _      = result should include("first sentence")
    result should include("Section 2. Second sentence.")
  }

  it should "fall back to body text for 'Formatted Text' HTML with no <pre>" in {
    val html   = "<html><body><h1>Title</h1><p>Body paragraph.</p></body></html>"
    val result = joined(bytesOf(html), "Formatted Text")
    val _      = result should include("Title")
    result should include("Body paragraph")
  }

  it should "skip <script> and <style> content for 'Formatted Text'" in {
    val html =
      """<html><body>
        |  <script>var leak = "should not appear";</script>
        |  <style>body { color: red; }</style>
        |  <p>Real bill content.</p>
        |</body></html>""".stripMargin

    val result = joined(bytesOf(html), "Formatted Text")
    val _      = result should include("Real bill content")
    val _      = result should not include "leak"
    result should not include "color: red"
  }

  it should "extract <legis-body> contents from 'Formatted XML' (USLM-style)" in {
    val xml =
      """<?xml version="1.0" encoding="UTF-8"?>
        |<bill>
        |  <metadata><docTitle>Skip me</docTitle></metadata>
        |  <form>Skip this header too</form>
        |  <legis-body>
        |    <section>SECTION 1. The actual bill content.</section>
        |    <section>SECTION 2. More content.</section>
        |  </legis-body>
        |</bill>""".stripMargin

    val result = joined(bytesOf(xml), "Formatted XML")
    val _      = result should include("SECTION 1. The actual bill content")
    val _      = result should include("SECTION 2. More content")
    val _      = result should not include "Skip me"
    result should not include "Skip this header"
  }

  it should "produce an empty stream for 'Formatted XML' with no <legis-body> (loud-failure design)" in {
    val xml =
      """<?xml version="1.0" encoding="UTF-8"?>
        |<bill><document>Old-format content here.</document></bill>""".stripMargin

    val emitted = TextExtractor.extractStream[IO](bytesOf(xml), "Formatted XML").compile.toList.unsafeRunSync()
    // No <legis-body> => extractor emits nothing. Downstream chunker emits nothing. The processor
    // logs "0 chunks" — a loud signal that the document didn't conform to expected USLM shape.
    emitted shouldBe Nil
  }

  it should "dispatch to PdfStreamExtractor for the 'PDF' format and return its extracted text" in {
    val expectedText = "SECTION 1. PDF dispatch test."
    val result       = joined(pdfBytes(expectedText), "PDF")
    result should include(expectedText)
  }

  it should "surface PdfExtractionFailed from PdfStreamExtractor when the 'PDF' dispatch hits invalid PDF bytes" in {
    val notAPdf = bytesOf("this is not a PDF")
    val attempt = TextExtractor.extractStream[IO](notAPdf, "PDF").compile.toList.attempt.unsafeRunSync()
    attempt match {
      case Left(_: PdfExtractionFailed) => succeed
      case other                        => fail(s"Expected PdfExtractionFailed, got $other")
    }
  }

  it should "decode bytes as plain UTF-8 text for the catch-all branch (unknown format)" in {
    val raw    = "Plain text bill body — line one.\nLine two."
    val result = joined(bytesOf(raw), "Plaintext")
    val _      = result should include("Plain text bill body")
    result should include("line one")
  }

  it should "collapse whitespace runs across all formats" in {
    val htmlWithLotsOfWhitespace = "<html><body><pre>  Section\t\t1.\n\n\n  Title.   </pre></body></html>"
    val result =
      TextExtractor
        .extractStream[IO](bytesOf(htmlWithLotsOfWhitespace), "Formatted Text")
        .compile
        .toList
        .unsafeRunSync()
        .mkString
    // Internal whitespace runs collapsed (no \n, no \t, no double-space).
    val _ = result should not include "\n"
    val _ = result should not include "\t"
    result should not include "  "
  }

  "collapseWhitespace" should "collapse runs of whitespace to single spaces" in {
    TextExtractor.collapseWhitespace("Section\t\n  1.\r\n\t Title") shouldBe "Section 1. Title"
  }

  it should "preserve a single leading or trailing space (does not trim)" in {
    val _ = TextExtractor.collapseWhitespace("  Section 1. Title  ") shouldBe " Section 1. Title "
    TextExtractor.collapseWhitespace("\nSection 1.") shouldBe " Section 1."
  }

  it should "preserve interior single spaces" in {
    TextExtractor.collapseWhitespace("a b c d") shouldBe "a b c d"
  }

  it should "collapse whitespace-only input to a single space" in {
    TextExtractor.collapseWhitespace("   \n\t  \r\n   ") shouldBe " "
  }

  it should "return empty string for empty input" in {
    TextExtractor.collapseWhitespace("") shouldBe ""
  }

}
