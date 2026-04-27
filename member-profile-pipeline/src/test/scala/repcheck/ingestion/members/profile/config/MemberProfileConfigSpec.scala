package repcheck.ingestion.members.profile.config

import scala.concurrent.duration._

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import repcheck.pipeline.models.errors.RetryConfig

class MemberProfileConfigSpec extends AnyFlatSpec with Matchers {

  private val testRetry = RetryConfig(
    maxRetries = 3,
    initialBackoffMs = 10,
    maxBackoffMs = 60000,
    backoffMultiplier = 2.0,
  )

  "MemberProfileConfig" should "hold congresses, parallelism, pageDelay, and eventPublishRetry" in {
    val config = MemberProfileConfig(
      congresses = List(117, 118, 119),
      parallelism = 4,
      pageDelay = 500.millis,
      eventPublishRetry = testRetry,
    )
    val _ = config.congresses shouldBe List(117, 118, 119)
    val _ = config.parallelism shouldBe 4
    val _ = config.pageDelay shouldBe 500.millis
    config.eventPublishRetry shouldBe testRetry
  }

  it should "accept an empty congresses list (DB-derived fallback path)" in {
    val config = MemberProfileConfig(
      congresses = Nil,
      parallelism = 1,
      pageDelay = 100.millis,
      eventPublishRetry = testRetry,
    )
    config.congresses shouldBe empty
  }

  it should "accept a single-congress list" in {
    val config = MemberProfileConfig(
      congresses = List(118),
      parallelism = 1,
      pageDelay = 100.millis,
      eventPublishRetry = testRetry,
    )
    config.congresses shouldBe List(118)
  }

  it should "allow single-threaded parallelism" in {
    val config = MemberProfileConfig(
      congresses = List(118),
      parallelism = 1,
      pageDelay = 100.millis,
      eventPublishRetry = testRetry,
    )
    config.parallelism shouldBe 1
  }

  it should "allow zero page delay" in {
    val config = MemberProfileConfig(
      congresses = List(118),
      parallelism = 4,
      pageDelay = Duration.Zero,
      eventPublishRetry = testRetry,
    )
    config.pageDelay shouldBe Duration.Zero
  }

}
