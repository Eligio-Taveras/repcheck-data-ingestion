package repcheck.ingestion.amendments.textcheck.config

import scala.concurrent.duration._

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import repcheck.pipeline.models.errors.RetryConfig

class AmendmentTextCheckerConfigSpec extends AnyFlatSpec with Matchers {

  "AmendmentTextCheckerConfig" should "have spec defaults" in {
    val cfg = AmendmentTextCheckerConfig()
    val _   = cfg.minCongress shouldBe 117
    val _   = cfg.staleAfter shouldBe 4.hours
    val _   = cfg.parallelism shouldBe 4
    val _   = cfg.pageDelay shouldBe 0.millis
    val _   = cfg.pageSize shouldBe 250
    cfg.eventPublishRetry shouldBe RetryConfig()
  }

  it should "accept values at the boundary" in {
    val cfg = AmendmentTextCheckerConfig(
      minCongress = 117,
      staleAfter = 1.millisecond,
      parallelism = 1,
      pageDelay = 0.millis,
      pageSize = 1,
    )
    val _ = cfg.minCongress shouldBe 117
    val _ = cfg.parallelism shouldBe 1
    cfg.pageSize shouldBe 1
  }

  it should "reject minCongress < 117" in {
    val ex = intercept[IllegalArgumentException](AmendmentTextCheckerConfig(minCongress = 116))
    ex.getMessage should include("minCongress")
  }

  it should "reject zero or negative staleAfter" in {
    val ex = intercept[IllegalArgumentException](AmendmentTextCheckerConfig(staleAfter = 0.seconds))
    ex.getMessage should include("staleAfter")
  }

  it should "reject parallelism < 1" in {
    val ex = intercept[IllegalArgumentException](AmendmentTextCheckerConfig(parallelism = 0))
    ex.getMessage should include("parallelism")
  }

  it should "reject pageSize < 1" in {
    val ex = intercept[IllegalArgumentException](AmendmentTextCheckerConfig(pageSize = 0))
    ex.getMessage should include("pageSize")
  }

}
