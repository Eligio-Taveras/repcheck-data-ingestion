package repcheck.ingestion.bills.text.embedding

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class EmbeddingGenerationFailedSpec extends AnyFlatSpec with Matchers {

  "EmbeddingGenerationFailed" should "include reason in message" in {
    val ex = EmbeddingGenerationFailed("model unavailable", 500)
    ex.getMessage should include("model unavailable")
  }

  it should "include text length in message" in {
    val ex = EmbeddingGenerationFailed("timeout", 1024)
    ex.getMessage should include("1024")
  }

  it should "be an Exception" in {
    val ex = EmbeddingGenerationFailed("error", 0)
    ex shouldBe a[Exception]
  }

}
