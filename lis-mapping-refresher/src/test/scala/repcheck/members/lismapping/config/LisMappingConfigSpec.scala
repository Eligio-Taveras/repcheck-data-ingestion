package repcheck.members.lismapping.config

import scala.concurrent.duration._

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class LisMappingConfigSpec extends AnyFlatSpec with Matchers {

  "LisMappingConfig" should "hold all configuration fields" in {
    val config = LisMappingConfig(
      currentCongress = 118,
      congressLookbackWindow = 5,
      parallelism = 1,
      requestTimeout = 30.seconds,
    )
    val _ = config.currentCongress shouldBe 118
    val _ = config.congressLookbackWindow shouldBe 5
    val _ = config.parallelism shouldBe 1
    config.requestTimeout shouldBe 30.seconds
  }

  it should "allow higher parallelism" in {
    val config = LisMappingConfig(
      currentCongress = 117,
      congressLookbackWindow = 3,
      parallelism = 4,
      requestTimeout = 60.seconds,
    )
    config.parallelism shouldBe 4
  }

  it should "allow minimal lookback window" in {
    val config = LisMappingConfig(
      currentCongress = 118,
      congressLookbackWindow = 1,
      parallelism = 1,
      requestTimeout = 10.seconds,
    )
    config.congressLookbackWindow shouldBe 1
  }

}
