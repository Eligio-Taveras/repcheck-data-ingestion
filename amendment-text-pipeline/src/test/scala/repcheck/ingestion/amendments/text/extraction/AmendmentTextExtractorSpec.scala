package repcheck.ingestion.amendments.text.extraction

import cats.effect.IO
import cats.effect.unsafe.implicits.global

import fs2.Stream

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import repcheck.ingestion.amendments.text.errors.AmendmentTextProcessingFailed

class AmendmentTextExtractorSpec extends AnyFlatSpec with Matchers {

  private val testNaturalKey = "117-SAMDT-2137"

  "extractStream" should "dispatch HTML to CrecHtmlExtractor" in {
    val html  = "<html><body><pre>plain amendment text</pre></body></html>"
    val bytes = Stream.emits(html.getBytes("UTF-8")).covary[IO]
    val text =
      AmendmentTextExtractor
        .extractStream[IO](bytes, "HTML", testNaturalKey)
        .compile
        .toList
        .unsafeRunSync()
        .mkString(" ")
    text should include("plain amendment text")
  }

  it should "raise AmendmentTextProcessingFailed for unknown formatType naming the natural key" in {
    val bytes = Stream.empty.covary[IO]
    val attempt = AmendmentTextExtractor
      .extractStream[IO](bytes, "Formatted XML", testNaturalKey)
      .compile
      .toList
      .attempt
      .unsafeRunSync()
    attempt match {
      case Left(err: AmendmentTextProcessingFailed) =>
        err.getMessage should include(testNaturalKey)
      case other => fail(s"Expected AmendmentTextProcessingFailed, got $other")
    }
  }

}
