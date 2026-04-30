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

}
