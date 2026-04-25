package repcheck.ingestion.bills.text.embedding

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class EmbeddingContextLengthExceededSpec extends AnyFlatSpec with Matchers {

  "EmbeddingContextLengthExceeded" should "format message with reason and totalLength" in {
    val ex = EmbeddingContextLengthExceeded("input length exceeds context length", 30000)
    val _  = ex.getMessage should include("30000")
    ex.getMessage should include("input length exceeds context length")
  }

  it should "expose reason field" in {
    val ex = EmbeddingContextLengthExceeded("oops", 12345)
    ex.reason shouldBe "oops"
  }

  it should "expose textLength field" in {
    val ex = EmbeddingContextLengthExceeded("oops", 12345)
    ex.textLength shouldBe 12345
  }

  it should "be an Exception subclass for cats-effect raiseError compatibility" in {
    val ex: Exception = EmbeddingContextLengthExceeded("oops", 0)
    ex shouldBe a[Exception]
  }

  it should "be distinguishable from EmbeddingGenerationFailed via pattern match" in {
    val ex: Throwable = EmbeddingContextLengthExceeded("oops", 0)
    val isContextErr = ex match {
      case _: EmbeddingContextLengthExceeded => true
      case _: EmbeddingGenerationFailed      => false
      case _                                 => false
    }
    isContextErr shouldBe true
  }

}
