package repcheck.ingestion.text.chunking

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class InvalidChunkSizeSpec extends AnyFlatSpec with Matchers {

  "InvalidChunkSize" should "include the misconfigured value in its message" in {
    InvalidChunkSize(0).getMessage should include("0")
  }

  it should "format negative values readably" in {
    InvalidChunkSize(-7).getMessage should include("-7")
  }

  it should "be a Throwable usable with Async[F].raiseError" in {
    val ex: Throwable = InvalidChunkSize(0)
    ex.getMessage shouldBe "TextChunker requires maxChunkChars > 0, got 0"
  }

}
