package repcheck.ingestion.bills.text.config

import scala.concurrent.duration._

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class BillTextPipelineConfigSpec extends AnyFlatSpec with Matchers {

  "BillTextPipelineConfig" should "hold parallelism setting" in {
    val config =
      BillTextPipelineConfig(
        parallelism = 4,
        downloadTimeoutSeconds = 60,
        pageDelay = 100.millis,
        govInfoApiKey = "test-key",
        govInfoBaseUrl = "https://api.govinfo.gov",
      )
    config.parallelism shouldBe 4
  }

  it should "hold download timeout" in {
    val config =
      BillTextPipelineConfig(
        parallelism = 1,
        downloadTimeoutSeconds = 30,
        pageDelay = 100.millis,
        govInfoApiKey = "test-key",
        govInfoBaseUrl = "https://api.govinfo.gov",
      )
    config.downloadTimeoutSeconds shouldBe 30
  }

  it should "hold page delay" in {
    val config =
      BillTextPipelineConfig(
        parallelism = 1,
        downloadTimeoutSeconds = 60,
        pageDelay = 250.millis,
        govInfoApiKey = "test-key",
        govInfoBaseUrl = "https://api.govinfo.gov",
      )
    config.pageDelay shouldBe 250.millis
  }

  it should "hold the GovInfo API key" in {
    val config =
      BillTextPipelineConfig(
        parallelism = 1,
        downloadTimeoutSeconds = 60,
        pageDelay = 100.millis,
        govInfoApiKey = "actual-key-value",
        govInfoBaseUrl = "https://api.govinfo.gov",
      )
    config.govInfoApiKey shouldBe "actual-key-value"
  }

  it should "hold the GovInfo base URL (overridable for tests)" in {
    val config =
      BillTextPipelineConfig(
        parallelism = 1,
        downloadTimeoutSeconds = 60,
        pageDelay = 100.millis,
        govInfoApiKey = "test-key",
        govInfoBaseUrl = "http://localhost:9999",
      )
    config.govInfoBaseUrl shouldBe "http://localhost:9999"
  }

}
