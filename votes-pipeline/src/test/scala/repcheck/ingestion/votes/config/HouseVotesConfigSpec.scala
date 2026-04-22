package repcheck.ingestion.votes.config

import scala.concurrent.duration._

import pureconfig.ConfigSource

import com.typesafe.config.ConfigFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class HouseVotesConfigSpec extends AnyFlatSpec with Matchers {

  "HouseVotesConfig" should "derive from a full HOCON block" in {
    val raw = """
      |house {
      |  congress = 119
      |  session = 1
      |  parallelism = 2
      |  page-delay = 3s
      |  lookback-days = 14
      |}
      |""".stripMargin

    val cfg = ConfigSource.fromConfig(ConfigFactory.parseString(raw)).at("house").loadOrThrow[HouseVotesConfig]
    val _   = cfg.congress shouldBe 119
    val _   = cfg.session shouldBe 1
    val _   = cfg.parallelism shouldBe 2
    val _   = cfg.pageDelay shouldBe 3.seconds
    cfg.lookbackDays shouldBe 14
  }

  // PureConfig's Scala 3 `derives ConfigReader` does NOT honor case-class default arguments — every field must be
  // present in HOCON or loading fails. Case-class defaults are still available for direct constructor usage from Scala
  // code (see the "case class accessors" test below) which is useful for wiring test fixtures and for downstream
  // places that build the config programmatically rather than from HOCON.
  it should "fail to load when required fields are omitted from HOCON" in {
    val raw = """
      |house {
      |  congress = 119
      |  session = 1
      |}
      |""".stripMargin

    val attempt = ConfigSource.fromConfig(ConfigFactory.parseString(raw)).at("house").load[HouseVotesConfig]
    attempt.isLeft shouldBe true
  }

  it should "expose all fields via the case class accessors for downstream use" in {
    val cfg = HouseVotesConfig(congress = 118, session = 2)
    val _   = cfg.congress shouldBe 118
    val _   = cfg.session shouldBe 2
    val _   = cfg.parallelism shouldBe 1
    val _   = cfg.pageDelay shouldBe 2.seconds
    cfg.lookbackDays shouldBe 7
  }

}
