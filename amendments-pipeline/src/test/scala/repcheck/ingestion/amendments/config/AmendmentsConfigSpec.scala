package repcheck.ingestion.amendments.config

import scala.concurrent.duration._

import pureconfig.ConfigSource

import com.typesafe.config.ConfigFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class AmendmentsConfigSpec extends AnyFlatSpec with Matchers {

  "AmendmentsConfig" should "load every field from a HOCON block" in {
    val raw = ConfigFactory.parseString(
      """
        |congresses-min = 117
        |congresses-max = 119
        |lookback-days = 14
        |parallelism = 8
        |page-delay = 100ms
        |max-recursion-depth = 5
        |page-size = 250
        |""".stripMargin
    )

    val cfg = ConfigSource.fromConfig(raw).loadOrThrow[AmendmentsConfig]
    val _   = cfg.congressesMin shouldBe 117
    val _   = cfg.congressesMax shouldBe 119
    val _   = cfg.lookbackDays shouldBe 14
    val _   = cfg.parallelism shouldBe 8
    val _   = cfg.pageDelay shouldBe 100.millis
    val _   = cfg.maxRecursionDepth shouldBe 5
    cfg.pageSize shouldBe 250
  }

  it should "apply case-class defaults when constructed without arguments" in {
    val cfg = AmendmentsConfig()
    val _   = cfg.congressesMin shouldBe 102
    val _   = cfg.congressesMax shouldBe 119
    val _   = cfg.lookbackDays shouldBe 7
    val _   = cfg.parallelism shouldBe 4
    val _   = cfg.pageDelay shouldBe 0.millis
    val _   = cfg.maxRecursionDepth shouldBe 10
    cfg.pageSize shouldBe 250
  }

  it should "expose congresses as a Range" in {
    val cfg = AmendmentsConfig(congressesMin = 117, congressesMax = 119)
    val _   = cfg.congresses shouldBe (117 to 119)
    cfg.congresses.toList shouldBe List(117, 118, 119)
  }

  it should "produce a single-element Range when min == max" in {
    val cfg = AmendmentsConfig(congressesMin = 119, congressesMax = 119)
    val _   = cfg.congresses.size shouldBe 1
    cfg.congresses.toList shouldBe List(119)
  }

  it should "reject congressesMin < 102 (Constants.MinAmendmentCongress)" in {
    val ex = intercept[IllegalArgumentException] {
      val _ = AmendmentsConfig(congressesMin = 101)
    }
    ex.getMessage should include("congressesMin")
  }

  it should "reject congressesMin = 0" in {
    val ex = intercept[IllegalArgumentException] {
      val _ = AmendmentsConfig(congressesMin = 0)
    }
    ex.getMessage should include("congressesMin")
  }

  it should "reject congressesMax < congressesMin" in {
    val ex = intercept[IllegalArgumentException] {
      val _ = AmendmentsConfig(congressesMin = 118, congressesMax = 117)
    }
    ex.getMessage should include("congressesMax")
  }

  it should "reject lookbackDays < 0" in {
    val ex = intercept[IllegalArgumentException] {
      val _ = AmendmentsConfig(lookbackDays = -1)
    }
    ex.getMessage should include("lookbackDays")
  }

  it should "accept lookbackDays = 0" in {
    val cfg = AmendmentsConfig(lookbackDays = 0)
    cfg.lookbackDays shouldBe 0
  }

  it should "accept lookbackDays = 999999" in {
    val cfg = AmendmentsConfig(lookbackDays = 999999)
    cfg.lookbackDays shouldBe 999999
  }

  it should "reject parallelism < 1" in {
    val ex = intercept[IllegalArgumentException] {
      val _ = AmendmentsConfig(parallelism = 0)
    }
    ex.getMessage should include("parallelism")
  }

  it should "reject maxRecursionDepth < 0" in {
    val ex = intercept[IllegalArgumentException] {
      val _ = AmendmentsConfig(maxRecursionDepth = -1)
    }
    ex.getMessage should include("maxRecursionDepth")
  }

  it should "accept maxRecursionDepth = 0" in {
    val cfg = AmendmentsConfig(maxRecursionDepth = 0)
    cfg.maxRecursionDepth shouldBe 0
  }

  it should "reject pageSize < 1" in {
    val ex = intercept[IllegalArgumentException] {
      val _ = AmendmentsConfig(pageSize = 0)
    }
    ex.getMessage should include("pageSize")
  }

  it should "reject HOCON with congressesMin = 50" in {
    val raw = ConfigFactory.parseString(
      """
        |congresses-min = 50
        |congresses-max = 119
        |lookback-days = 7
        |parallelism = 4
        |page-delay = 0ms
        |max-recursion-depth = 10
        |page-size = 250
        |""".stripMargin
    )
    val ex = intercept[Throwable] {
      val _ = ConfigSource.fromConfig(raw).loadOrThrow[AmendmentsConfig]
    }
    ex.getMessage should include("congressesMin")
  }

}
