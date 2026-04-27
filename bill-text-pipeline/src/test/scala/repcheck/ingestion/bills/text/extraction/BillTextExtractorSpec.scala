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
 * Specs for [[BillTextExtractor]] — covers the format-dispatch + per-format extraction logic that previously lived
 * inside `BillTextDownloader.extractText`. Phase 2 of the bill-text-10mb plan moved these to a temp-file-friendly
 * extractor that reads from disk rather than from a heap-buffered String.
 *
 * Each test writes a small fixture to a temp file and asserts the extractor's behaviour. The temp files are cleaned up
 * via `try/finally` because we want to exercise the extractor in isolation, not through the streaming-download Resource
 * lifecycle (that's tested in `BillTextDownloaderSpec`).
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

  /**
   * Build a one-page PDF in memory and write it to a temp file. Mirrors the helper in [[PDFTextExtractorSpec]] (kept
   * inline rather than extracted to a shared util because the only reason both tests need it is the BillTextExtractor
   * `case "PDF"` dispatch verification — duplication is cheaper than a third file just for one helper).
   */
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

  "extract" should "extract the <pre> contents from a 'Formatted Text' HTML body" in {
    val html =
      """<html>
        |  <body>
        |    <pre>SECTION 1. Title — first sentence.
        |Section 2. Second sentence.</pre>
        |  </body>
        |</html>""".stripMargin

    withTempFile(html, ".html") { path =>
      val result = BillTextExtractor.extract[IO](path, "Formatted Text").unsafeRunSync()
      val _      = result should include("SECTION 1. Title")
      val _      = result should include("first sentence")
      result should include("Section 2. Second sentence.")
    }
  }

  it should "fall back to body text when 'Formatted Text' HTML has no <pre>" in {
    val html = "<html><body><h1>Title</h1><p>Body paragraph.</p></body></html>"

    withTempFile(html, ".html") { path =>
      val result = BillTextExtractor.extract[IO](path, "Formatted Text").unsafeRunSync()
      val _      = result should include("Title")
      result should include("Body paragraph")
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
      val result = BillTextExtractor.extract[IO](path, "Formatted XML").unsafeRunSync()
      val _      = result should include("SECTION 1. The actual bill content")
      val _      = result should include("SECTION 2. More content")
      val _      = result should not include "Skip me"
      result should not include "Skip this header"
    }
  }

  it should "fall back to whole-document text when 'Formatted XML' has no <legis-body>" in {
    val xml =
      """<?xml version="1.0" encoding="UTF-8"?>
        |<bill>
        |  <document>Old-format content here.</document>
        |</bill>""".stripMargin

    withTempFile(xml, ".xml") { path =>
      val result = BillTextExtractor.extract[IO](path, "Formatted XML").unsafeRunSync()
      result should include("Old-format content here")
    }
  }

  it should "dispatch to PDFTextExtractor for the 'PDF' format and return its extracted text" in {
    val expectedText = "SECTION 1. PDF dispatch test."
    val path         = writeTinyPdf(expectedText)
    try {
      val result = BillTextExtractor.extract[IO](path, "PDF").unsafeRunSync()
      result should include(expectedText)
    } finally {
      val _ = Files.deleteIfExists(path)
    }
  }

  it should "surface PdfExtractionFailed from PDFTextExtractor when the 'PDF' format dispatch hits an invalid PDF" in {
    val path = Files.createTempFile("bill-text-extractor-bad-pdf-", ".pdf")
    try {
      val _       = Files.writeString(path, "this is not a PDF")
      val attempt = BillTextExtractor.extract[IO](path, "PDF").attempt.unsafeRunSync()
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
      val result = BillTextExtractor.extract[IO](path, "Plaintext").unsafeRunSync()
      val _      = result should include("Plain text bill body")
      result should include("line one")
    }
  }

  it should "normalize whitespace across all formats" in {
    val htmlWithLotsOfWhitespace = "<html><body><pre>  Section\t\t1.\n\n\n  Title.   </pre></body></html>"
    withTempFile(htmlWithLotsOfWhitespace, ".html") { path =>
      val result = BillTextExtractor.extract[IO](path, "Formatted Text").unsafeRunSync()
      // Single-spaced and trimmed.
      val _ = result should not include "\n"
      val _ = result should not include "\t"
      val _ = result should not startWith " "
      result should not endWith " "
    }
  }

  "normalizeWhitespace" should "collapse runs of whitespace to single spaces" in {
    BillTextExtractor.normalizeWhitespace("Section\t\n  1.\r\n\t Title") shouldBe "Section 1. Title"
  }

  it should "trim leading and trailing whitespace" in {
    BillTextExtractor.normalizeWhitespace("  \n\t  Section 1. Title \n  ") shouldBe "Section 1. Title"
  }

  it should "preserve interior single spaces" in {
    BillTextExtractor.normalizeWhitespace("a b c d") shouldBe "a b c d"
  }

  it should "return empty string for whitespace-only input" in {
    BillTextExtractor.normalizeWhitespace("   \n\t  \r\n   ") shouldBe ""
  }

  it should "return empty string for empty input" in {
    BillTextExtractor.normalizeWhitespace("") shouldBe ""
  }

}
