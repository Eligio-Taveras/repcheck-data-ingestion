package repcheck.ingestion.bills.text.embedding

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class EmbeddingConfigSpec extends AnyFlatSpec with Matchers {

  "EmbeddingConfig" should "hold base URL" in {
    val config = EmbeddingConfig(
      baseUrl = "http://localhost:11434",
      modelName = "qwen3-embedding",
      dimensions = 1536,
      timeoutSeconds = 30,
    )
    config.baseUrl shouldBe "http://localhost:11434"
  }

  it should "hold model name" in {
    val config = EmbeddingConfig(
      baseUrl = "http://localhost:11434",
      modelName = "qwen3-embedding:4b",
      dimensions = 1536,
      timeoutSeconds = 30,
    )
    config.modelName shouldBe "qwen3-embedding:4b"
  }

  it should "hold dimensions" in {
    val config = EmbeddingConfig(
      baseUrl = "http://localhost:11434",
      modelName = "qwen3-embedding",
      dimensions = 1536,
      timeoutSeconds = 30,
    )
    config.dimensions shouldBe 1536
  }

  it should "hold timeout" in {
    val config = EmbeddingConfig(
      baseUrl = "http://localhost:11434",
      modelName = "qwen3-embedding",
      dimensions = 1536,
      timeoutSeconds = 60,
    )
    config.timeoutSeconds shouldBe 60
  }

}
