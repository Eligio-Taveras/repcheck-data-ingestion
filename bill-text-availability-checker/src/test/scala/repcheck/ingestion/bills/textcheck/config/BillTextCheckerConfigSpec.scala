package repcheck.ingestion.bills.textcheck.config

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import repcheck.pipeline.models.errors.RetryConfig

class BillTextCheckerConfigSpec extends AnyFlatSpec with Matchers {

  private val defaultRetry =
    RetryConfig(maxRetries = 3, initialBackoffMs = 10L, maxBackoffMs = 60000L, backoffMultiplier = 2.0)

  "BillTextCheckerConfig" should "hold parallelism setting" in {
    val config = BillTextCheckerConfig(parallelism = 4, eventPublishRetry = defaultRetry, congresses = "")
    config.parallelism shouldBe 4
  }

  it should "allow single-threaded parallelism" in {
    val config = BillTextCheckerConfig(parallelism = 1, eventPublishRetry = defaultRetry, congresses = "")
    config.parallelism shouldBe 1
  }

  it should "hold event publish retry configuration" in {
    val retry  = RetryConfig(maxRetries = 5, initialBackoffMs = 100L, maxBackoffMs = 30000L, backoffMultiplier = 1.5)
    val config = BillTextCheckerConfig(parallelism = 4, eventPublishRetry = retry, congresses = "")
    val _      = config.eventPublishRetry.maxRetries shouldBe 5
    config.eventPublishRetry.initialBackoffMs shouldBe 100L
  }

  "congressList" should "return empty list for empty string" in {
    val config = BillTextCheckerConfig(parallelism = 1, eventPublishRetry = defaultRetry, congresses = "")
    config.congressList shouldBe List.empty
  }

  it should "parse a single-value comma-free string" in {
    val config = BillTextCheckerConfig(parallelism = 1, eventPublishRetry = defaultRetry, congresses = "118")
    config.congressList shouldBe List(118)
  }

  it should "parse a comma-separated list" in {
    val config = BillTextCheckerConfig(parallelism = 1, eventPublishRetry = defaultRetry, congresses = "103,104,105")
    config.congressList shouldBe List(103, 104, 105)
  }

  it should "tolerate whitespace around tokens" in {
    val config =
      BillTextCheckerConfig(parallelism = 1, eventPublishRetry = defaultRetry, congresses = " 117 , 118 ,  119 ")
    config.congressList shouldBe List(117, 118, 119)
  }

  it should "silently drop non-numeric tokens" in {
    val config =
      BillTextCheckerConfig(parallelism = 1, eventPublishRetry = defaultRetry, congresses = "117,bogus,119")
    config.congressList shouldBe List(117, 119)
  }

}
