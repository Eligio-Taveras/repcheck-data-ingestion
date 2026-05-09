package repcheck.ingestion.amendments.text.config

import scala.concurrent.duration.DurationInt

import pureconfig.ConfigSource

import com.typesafe.config.ConfigFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class AmendmentTextPipelineConfigSpec extends AnyFlatSpec with Matchers {

  "AmendmentTextPipelineConfig" should "load with defaults from a complete HOCON snippet" in {
    val raw = ConfigFactory.parseString("""
      parallelism = 2
      download-timeout-seconds = 30
      page-delay = 50ms
      gov-info-api-key = "my-key"
      gov-info-base-url = "https://api.govinfo.gov"
    """)
    val config = ConfigSource.fromConfig(raw).loadOrThrow[AmendmentTextPipelineConfig]
    val _      = config.parallelism shouldBe 2
    val _      = config.downloadTimeoutSeconds shouldBe 30
    val _      = config.pageDelay shouldBe 50.millis
    val _      = config.govInfoApiKey shouldBe "my-key"
    config.govInfoBaseUrl shouldBe "https://api.govinfo.gov"
  }

  it should "fail to load when a required field is missing" in {
    val raw = ConfigFactory.parseString("""
      parallelism = 2
      download-timeout-seconds = 30
      page-delay = 50ms
      gov-info-base-url = "https://api.govinfo.gov"
    """)
    val attempt = ConfigSource.fromConfig(raw).load[AmendmentTextPipelineConfig]
    attempt.isLeft shouldBe true
  }

}
