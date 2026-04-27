package repcheck.ingestion.bills.text.extraction

import java.io.ByteArrayOutputStream
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}

import cats.effect.IO
import cats.effect.unsafe.implicits.global

import org.apache.pdfbox.pdmodel.common.PDRectangle
import org.apache.pdfbox.pdmodel.font.{PDType1Font, Standard14Fonts}
import org.apache.pdfbox.pdmodel.{PDDocument, PDPage, PDPageContentStream}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import repcheck.ingestion.bills.text.errors.PdfExtractionFailed

/**
 * Specs for the streaming [[BillTextExtractor]] dispatcher. Phase 3 of the bill-text-10mb plan replaces the buffered
 * `extract: F[String]` API with `extractStream: Stream[F, String]`, with per-format streaming extractors backing each
 * branch.
 *
 * Tests consume each stream via `compile.toList` and assert the joined fragments contain the expected text. We don't
 * assert exact fragment counts — those are parser-implementation details that vary between StAX, TagSoup, and PDFBox,
 * and asserting them would couple tests to internals.
 */
class BillTextExtractorSpec extends AnyFlatSpec with Matchers {

  private def withTempFile[A](contents: String, suffix: String)(f: Path => A): A = {
    val path = Files.createTempFile("bill-text-extractor-test-", suffix)
    try {
      val _ = Files.writeString(path, contents, StandardCharsets.UTF_8)
      f(path)
    } finally {
      val _ = Files.deleteIfExists(path)
    }
  }

  private def writeTinyPdf(text: String): Path = {
    val document = new PDDocument()
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

      val baos = new ByteArrayOutputStream()
      try
        document.save(baos)
      finally
        baos.close()

      val path = Files.createTempFile("bill-text-extractor-pdf-", ".pdf")
      val _    = Files.write(path, baos.toByteArray)
      path
    } finally document.close()
  }

  private def joined(path: Path, format: String): String =
    BillTextExtractor
      .extractStream[IO](path, format)
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

    withTempFile(html, ".html") { path =>
      val result = joined(path, "Formatted Text")
      val _      = result should include("SECTION 1. Title")
      val _      = result should include("first sentence")
      result should include("Section 2. Second sentence.")
    }
  }

  it should "fall back to body text for 'Formatted Text' HTML with no <pre>" in {
    val html = "<html><body><h1>Title</h1><p>Body paragraph.</p></body></html>"

    withTempFile(html, ".html") { path =>
      val result = joined(path, "Formatted Text")
      val _      = result should include("Title")
      result should include("Body paragraph")
    }
  }

  it should "skip <script> and <style> content for 'Formatted Text'" in {
    val html =
      """<html><body>
        |  <script>var leak = "should not appear";</script>
        |  <style>body { color: red; }</style>
        |  <p>Real bill content.</p>
        |</body></html>""".stripMargin

    withTempFile(html, ".html") { path =>
      val result = joined(path, "Formatted Text")
      val _      = result should include("Real bill content")
      val _      = result should not include "leak"
      result should not include "color: red"
    }
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

    withTempFile(xml, ".xml") { path =>
      val result = joined(path, "Formatted XML")
      val _      = result should include("SECTION 1. The actual bill content")
      val _      = result should include("SECTION 2. More content")
      val _      = result should not include "Skip me"
      result should not include "Skip this header"
    }
  }

  it should "produce an empty stream for 'Formatted XML' with no <legis-body> (loud-failure design)" in {
    val xml =
      """<?xml version="1.0" encoding="UTF-8"?>
        |<bill><document>Old-format content here.</document></bill>""".stripMargin

    withTempFile(xml, ".xml") { path =>
      val emitted = BillTextExtractor.extractStream[IO](path, "Formatted XML").compile.toList.unsafeRunSync()
      // No <legis-body> => extractor emits nothing. Downstream chunker emits nothing. The processor
      // logs "0 chunks" — a loud signal that the document didn't conform to expected USLM shape.
      emitted shouldBe Nil
    }
  }

  it should "dispatch to PdfStreamExtractor for the 'PDF' format and return its extracted text" in {
    val expectedText = "SECTION 1. PDF dispatch test."
    val path         = writeTinyPdf(expectedText)
    try {
      val result = joined(path, "PDF")
      result should include(expectedText)
    } finally {
      val _ = Files.deleteIfExists(path)
    }
  }

  it should "surface PdfExtractionFailed from PdfStreamExtractor when the 'PDF' dispatch hits an invalid PDF" in {
    val path = Files.createTempFile("bill-text-extractor-bad-pdf-", ".pdf")
    try {
      val _       = Files.writeString(path, "this is not a PDF")
      val attempt = BillTextExtractor.extractStream[IO](path, "PDF").compile.toList.attempt.unsafeRunSync()
      attempt match {
        case Left(_: PdfExtractionFailed) => succeed
        case other                        => fail(s"Expected PdfExtractionFailed, got $other")
      }
    } finally {
      val _ = Files.deleteIfExists(path)
    }
  }

  it should "read the file as plain UTF-8 text for the catch-all branch (unknown format)" in {
    val raw = "Plain text bill body — line one.\nLine two."

    withTempFile(raw, ".txt") { path =>
      val result = joined(path, "Plaintext")
      val _      = result should include("Plain text bill body")
      result should include("line one")
    }
  }

  it should "collapse whitespace runs across all formats" in {
    val htmlWithLotsOfWhitespace = "<html><body><pre>  Section\t\t1.\n\n\n  Title.   </pre></body></html>"
    withTempFile(htmlWithLotsOfWhitespace, ".html") { path =>
      val result =
        BillTextExtractor.extractStream[IO](path, "Formatted Text").compile.toList.unsafeRunSync().mkString
      // Internal whitespace runs collapsed (no \n, no \t, no double-space).
      val _ = result should not include "\n"
      val _ = result should not include "\t"
      result should not include "  "
    }
  }

  "collapseWhitespace" should "collapse runs of whitespace to single spaces" in {
    BillTextExtractor.collapseWhitespace("Section\t\n  1.\r\n\t Title") shouldBe "Section 1. Title"
  }

  it should "preserve a single leading or trailing space (does not trim)" in {
    val _ = BillTextExtractor.collapseWhitespace("  Section 1. Title  ") shouldBe " Section 1. Title "
    BillTextExtractor.collapseWhitespace("\nSection 1.") shouldBe " Section 1."
  }

  it should "preserve interior single spaces" in {
    BillTextExtractor.collapseWhitespace("a b c d") shouldBe "a b c d"
  }

  it should "collapse whitespace-only input to a single space" in {
    BillTextExtractor.collapseWhitespace("   \n\t  \r\n   ") shouldBe " "
  }

  it should "return empty string for empty input" in {
    BillTextExtractor.collapseWhitespace("") shouldBe ""
  }

}
