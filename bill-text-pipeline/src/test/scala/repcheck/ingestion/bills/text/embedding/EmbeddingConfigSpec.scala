package repcheck.ingestion.bills.text.embedding

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class EmbeddingConfigSpec extends AnyFlatSpec with Matchers {

  "EmbeddingConfig" should "hold base URL" in {
    val config = EmbeddingConfig(
      baseUrl = "http://localhost:11434",
      modelName = "bill-text-embedding",
      dimensions = 1024,
      timeoutSeconds = 30,
      maxChunkChars = 30000,
      embedBatchSize = 50,
    )
    config.baseUrl shouldBe "http://localhost:11434"
  }

  it should "hold model name" in {
    val config = EmbeddingConfig(
      baseUrl = "http://localhost:11434",
      modelName = "qwen3-embedding:0.6b",
      dimensions = 1024,
      timeoutSeconds = 30,
      maxChunkChars = 30000,
      embedBatchSize = 50,
    )
    config.modelName shouldBe "qwen3-embedding:0.6b"
  }

  it should "hold dimensions" in {
    val config = EmbeddingConfig(
      baseUrl = "http://localhost:11434",
      modelName = "bill-text-embedding",
      dimensions = 1024,
      timeoutSeconds = 30,
      maxChunkChars = 30000,
      embedBatchSize = 50,
    )
    config.dimensions shouldBe 1024
  }

  it should "hold timeout" in {
    val config = EmbeddingConfig(
      baseUrl = "http://localhost:11434",
      modelName = "bill-text-embedding",
      dimensions = 1024,
      timeoutSeconds = 60,
      maxChunkChars = 30000,
      embedBatchSize = 50,
    )
    config.timeoutSeconds shouldBe 60
  }

  it should "hold maxChunkChars" in {
    val config = EmbeddingConfig(
      baseUrl = "http://localhost:11434",
      modelName = "bill-text-embedding",
      dimensions = 1024,
      timeoutSeconds = 30,
      maxChunkChars = 12000,
      embedBatchSize = 50,
    )
    config.maxChunkChars shouldBe 12000
  }

  it should "hold embedBatchSize" in {
    val config = EmbeddingConfig(
      baseUrl = "http://localhost:11434",
      modelName = "bill-text-embedding",
      dimensions = 1024,
      timeoutSeconds = 30,
      maxChunkChars = 12000,
      embedBatchSize = 50,
    )
    config.embedBatchSize shouldBe 50
  }

}
