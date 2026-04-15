package repcheck.members.lismapping.config

import scala.concurrent.duration._

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class LisMappingConfigSpec extends AnyFlatSpec with Matchers {

  "LisMappingConfig" should "hold parallelism and requestTimeout" in {
    val config = LisMappingConfig(parallelism = 1, requestTimeout = 30.seconds)
    val _      = config.parallelism shouldBe 1
    config.requestTimeout shouldBe 30.seconds
  }

  it should "allow higher parallelism" in {
    val config = LisMappingConfig(parallelism = 4, requestTimeout = 60.seconds)
    config.parallelism shouldBe 4
  }

  it should "allow short request timeout" in {
    val config = LisMappingConfig(parallelism = 1, requestTimeout = 5.seconds)
    config.requestTimeout shouldBe 5.seconds
  }

}
