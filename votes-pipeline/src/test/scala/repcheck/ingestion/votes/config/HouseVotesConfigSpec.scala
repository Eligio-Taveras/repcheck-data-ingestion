package repcheck.ingestion.votes.config

import scala.concurrent.duration._

import pureconfig.ConfigSource

import com.typesafe.config.ConfigFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class HouseVotesConfigSpec extends AnyFlatSpec with Matchers {

  "HouseVotesConfig" should "derive from a full HOCON block" in {
    // Per P6.H5 the (congress, session) tuple is no longer part of HouseVotesConfig — it's resolved upstream and
    // passed to fetchRecentVotes at call time. This block exercises only the fields the case class actually carries.
    val raw = """
      |house {
      |  parallelism = 2
      |  permits = 4
      |  page-delay = 3s
      |  lookback-days = 14
      |}
      |""".stripMargin

    val cfg = ConfigSource.fromConfig(ConfigFactory.parseString(raw)).at("house").loadOrThrow[HouseVotesConfig]
    val _   = cfg.parallelism shouldBe 2
    val _   = cfg.permits shouldBe 4
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
      |  parallelism = 2
      |}
      |""".stripMargin

    val attempt = ConfigSource.fromConfig(ConfigFactory.parseString(raw)).at("house").load[HouseVotesConfig]
    attempt.isLeft shouldBe true
  }

  it should "expose all fields via the case class accessors for downstream use" in {
    val cfg = HouseVotesConfig()
    val _   = cfg.parallelism shouldBe 1
    val _   = cfg.permits shouldBe 1
    val _   = cfg.pageDelay shouldBe 2.seconds
    cfg.lookbackDays shouldBe 7
  }

}
