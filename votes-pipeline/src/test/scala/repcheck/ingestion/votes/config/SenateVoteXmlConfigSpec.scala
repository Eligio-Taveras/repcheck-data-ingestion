package repcheck.ingestion.votes.config

import scala.concurrent.duration._

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SenateVoteXmlConfigSpec extends AnyFlatSpec with Matchers {

  "SenateVoteXmlConfig" should "default baseUrl to senate.gov's LIS root (SenateVoteUrls adds roll_call_* sub-paths)" in {
    val config = SenateVoteXmlConfig()
    config.baseUrl shouldBe "https://www.senate.gov/legislative/LIS"
  }

  it should "default parallelism to 1 (per-plan decision: senate.gov is rate-sensitive)" in {
    val config = SenateVoteXmlConfig()
    config.parallelism shouldBe 1
  }

  it should "default requestDelay to 3 seconds" in {
    val config = SenateVoteXmlConfig()
    config.requestDelay shouldBe 3.seconds
  }

  it should "permit overrides at construction time" in {
    val config = SenateVoteXmlConfig(
      baseUrl = "http://example",
      parallelism = 4,
      requestDelay = 500.millis,
    )
    val _ = config.baseUrl shouldBe "http://example"
    val _ = config.parallelism shouldBe 4
    config.requestDelay shouldBe 500.millis
  }

}
