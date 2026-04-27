package repcheck.ingestion.bills.text.errors

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class PdfExtractionFailedSpec extends AnyFlatSpec with Matchers {

  "PdfExtractionFailed" should "include the path and reason in the message" in {
    val cause = new RuntimeException("PDFBox underlying error")
    val err   = PdfExtractionFailed("/tmp/x.pdf", "Document is encrypted", cause)
    val _     = err.getMessage should include("/tmp/x.pdf")
    err.getMessage should include("Document is encrypted")
  }

  it should "preserve the underlying cause" in {
    val cause = new RuntimeException("boom")
    val err   = PdfExtractionFailed("/tmp/y.pdf", "boom", cause)
    err.getCause shouldBe cause
  }

  it should "expose path and reason as case-class fields" in {
    val err = PdfExtractionFailed("/tmp/z.pdf", "encrypted", new RuntimeException(""))
    val _   = err.path shouldBe "/tmp/z.pdf"
    err.reason shouldBe "encrypted"
  }

  it should "be a Throwable subclass" in {
    val _: Throwable = PdfExtractionFailed("/tmp/x.pdf", "reason", new RuntimeException(""))
    succeed
  }

}
