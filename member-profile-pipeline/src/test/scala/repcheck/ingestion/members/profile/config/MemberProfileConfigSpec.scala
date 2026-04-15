package repcheck.ingestion.members.profile.config

import scala.concurrent.duration._

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class MemberProfileConfigSpec extends AnyFlatSpec with Matchers {

  "MemberProfileConfig" should "hold parallelism and pageDelay" in {
    val config = MemberProfileConfig(parallelism = 4, pageDelay = 500.millis)
    val _      = config.parallelism shouldBe 4
    config.pageDelay shouldBe 500.millis
  }

  it should "allow single-threaded parallelism" in {
    val config = MemberProfileConfig(parallelism = 1, pageDelay = 100.millis)
    config.parallelism shouldBe 1
  }

  it should "allow zero page delay" in {
    val config = MemberProfileConfig(parallelism = 4, pageDelay = Duration.Zero)
    config.pageDelay shouldBe Duration.Zero
  }

}
