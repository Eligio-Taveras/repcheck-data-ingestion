package repcheck.ingestion.amendments.text.config

import scala.concurrent.duration.DurationInt

import pureconfig.ConfigSource

import com.typesafe.config.ConfigFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class AmendmentTextPipelineConfigSpec extends AnyFlatSpec with Matchers {

  "AmendmentTextPipelineConfig" should "load from a complete HOCON snippet with explicit http blocks + permits" in {
    // PureConfig's Scala 3 derivation does not honor case-class defaults when the corresponding HOCON key is
    // absent (it currently surfaces as "Key not found"). The production application.conf provides every field
    // explicitly with `${?ENV}` overrides, so this spec mirrors that shape — defaults-from-code are still useful
    // when the case class is constructed programmatically (e.g., the testConfig in AmendmentTextPipelinePipelineSpec).
    val raw = ConfigFactory.parseString("""
      parallelism = 2
      page-delay = 50ms
      gov-info-api-key = "my-key"
      gov-info-base-url = "https://api.govinfo.gov"
      gov-info-permits = 2
      gov-info-http {
        connect-timeout = 10s
        request-timeout = 120s
        max-total-connections = 10
        idle-timeout = 120s
      }
      ollama-http {
        connect-timeout = 10s
        request-timeout = 120s
        max-total-connections = 10
        idle-timeout = 60s
      }
    """)
    val config = ConfigSource.fromConfig(raw).loadOrThrow[AmendmentTextPipelineConfig]
    val _      = config.parallelism shouldBe 2
    val _      = config.pageDelay shouldBe 50.millis
    val _      = config.govInfoApiKey shouldBe "my-key"
    val _      = config.govInfoBaseUrl shouldBe "https://api.govinfo.gov"
    val _      = config.govInfoPermits shouldBe 2L
    val _      = config.govInfoHttp.requestTimeout shouldBe 120.seconds
    val _      = config.govInfoHttp.idleTimeout shouldBe 120.seconds
    val _      = config.ollamaHttp.requestTimeout shouldBe 120.seconds
    config.ollamaHttp.idleTimeout shouldBe 60.seconds
  }

  it should "honor explicit overrides for permits + http timeouts" in {
    // HttpClientConfig has 4 required-ish fields (connect-timeout, request-timeout, max-total-connections,
    // idle-timeout). PureConfig's derived reader requires all of them when the block is present in HOCON, even
    // though the case class supplies defaults — so the test here mirrors what the real application.conf provides.
    val raw = ConfigFactory.parseString("""
      parallelism = 2
      page-delay = 50ms
      gov-info-api-key = "my-key"
      gov-info-base-url = "https://api.govinfo.gov"
      gov-info-permits = 5
      gov-info-http {
        connect-timeout = 10s
        request-timeout = 45s
        max-total-connections = 10
        idle-timeout = 90s
      }
      ollama-http {
        connect-timeout = 10s
        request-timeout = 30s
        max-total-connections = 10
        idle-timeout = 30s
      }
    """)
    val config = ConfigSource.fromConfig(raw).loadOrThrow[AmendmentTextPipelineConfig]
    val _      = config.govInfoPermits shouldBe 5L
    val _      = config.govInfoHttp.requestTimeout shouldBe 45.seconds
    val _      = config.govInfoHttp.idleTimeout shouldBe 90.seconds
    val _      = config.ollamaHttp.requestTimeout shouldBe 30.seconds
    config.ollamaHttp.idleTimeout shouldBe 30.seconds
  }

  it should "fail to load when a required field is missing" in {
    val raw = ConfigFactory.parseString("""
      parallelism = 2
      page-delay = 50ms
      gov-info-base-url = "https://api.govinfo.gov"
    """)
    val attempt = ConfigSource.fromConfig(raw).load[AmendmentTextPipelineConfig]
    attempt.isLeft shouldBe true
  }

}
