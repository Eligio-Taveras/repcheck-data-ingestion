package repcheck.members.lismapping.config

import scala.concurrent.duration._

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class LisMappingConfigSpec extends AnyFlatSpec with Matchers {

  private def defaults(
    parallelism: Int = 1,
    requestTimeout: FiniteDuration = 30.seconds,
    currentCongress: Int = 118,
    congressLookbackWindow: Int = 5,
    senatorXmlUrl: String = "https://www.senate.gov/about/senator-lookup.xml",
  ): LisMappingConfig =
    LisMappingConfig(
      parallelism = parallelism,
      requestTimeout = requestTimeout,
      currentCongress = currentCongress,
      congressLookbackWindow = congressLookbackWindow,
      senatorXmlUrl = senatorXmlUrl,
    )

  "LisMappingConfig" should "hold parallelism and requestTimeout" in {
    val config = defaults()
    val _      = config.parallelism shouldBe 1
    config.requestTimeout shouldBe 30.seconds
  }

  it should "allow higher parallelism" in {
    val config = defaults(parallelism = 4, requestTimeout = 60.seconds)
    config.parallelism shouldBe 4
  }

  it should "allow short request timeout" in {
    val config = defaults(requestTimeout = 5.seconds)
    config.requestTimeout shouldBe 5.seconds
  }

  it should "carry the congress lookback window" in {
    val config = defaults(currentCongress = 118, congressLookbackWindow = 5)
    val _      = config.currentCongress shouldBe 118
    config.congressLookbackWindow shouldBe 5
  }

  it should "allow customizing the senate XML URL" in {
    val config = defaults(senatorXmlUrl = "http://example.com/senators.xml")
    config.senatorXmlUrl shouldBe "http://example.com/senators.xml"
  }

}
