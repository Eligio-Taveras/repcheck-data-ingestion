package repcheck.ingestion.bills.text.extraction

import java.io.ByteArrayOutputStream
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
 * Specs for [[PDFTextExtractor]] — covers the PDFBox-backed PDF extraction. Phase 2 of the bill-text-10mb plan
 * introduced this module to fix the prior `case "PDF" => content` bug where raw PDF bytes got coerced into a UTF-8
 * String and stored as garbage in `raw_bill_text.content`.
 *
 * Builds a tiny single-page PDF in memory using PDFBox's own document API, writes it to disk, then extracts. This
 * avoids checking in binary fixtures and makes the test self-contained.
 */
class PDFTextExtractorSpec extends AnyFlatSpec with Matchers {

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

      val path = Files.createTempFile("pdf-text-extractor-test-", ".pdf")
      val _    = Files.write(path, baos.toByteArray)
      path
    } finally document.close()
  }

  "extract" should "return text content for a valid PDF" in {
    val expectedText = "SECTION 1. Title."
    val path         = writeTinyPdf(expectedText)
    try {
      val extracted = PDFTextExtractor.extract[IO](path).unsafeRunSync()
      extracted should include(expectedText)
    } finally {
      val _ = Files.deleteIfExists(path)
    }
  }

  it should "raise PdfExtractionFailed when the file isn't a valid PDF" in {
    val path = Files.createTempFile("not-a-pdf-", ".pdf")
    try {
      // Write garbage bytes (no %PDF- header) so PDFBox throws on parse.
      val _ = Files.writeString(path, "this is definitely not a PDF document")

      val attempt = PDFTextExtractor.extract[IO](path).attempt.unsafeRunSync()

      attempt match {
        case Left(_: PdfExtractionFailed) => succeed
        case other                        => fail(s"Expected PdfExtractionFailed, got $other")
      }
    } finally {
      val _ = Files.deleteIfExists(path)
    }
  }

  it should "raise PdfExtractionFailed when the file is missing" in {
    val nonExistent = java.nio.file.Paths.get(System.getProperty("java.io.tmpdir"), "definitely-does-not-exist.pdf")
    val attempt     = PDFTextExtractor.extract[IO](nonExistent).attempt.unsafeRunSync()

    attempt match {
      case Left(_: PdfExtractionFailed) => succeed
      case other                        => fail(s"Expected PdfExtractionFailed, got $other")
    }
  }

  it should "preserve the path string on failure for log debugging" in {
    val path = Files.createTempFile("garbage-", ".pdf")
    try {
      val _       = Files.writeString(path, "garbage")
      val attempt = PDFTextExtractor.extract[IO](path).attempt.unsafeRunSync()

      attempt match {
        case Left(err: PdfExtractionFailed) =>
          err.path shouldBe path.toString
        case other => fail(s"Expected PdfExtractionFailed, got $other")
      }
    } finally {
      val _ = Files.deleteIfExists(path)
    }
  }

}
