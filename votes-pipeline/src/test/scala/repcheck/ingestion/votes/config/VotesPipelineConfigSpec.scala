package repcheck.ingestion.votes.config

import scala.concurrent.duration._

import pureconfig.ConfigSource

import com.typesafe.config.ConfigFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class VotesPipelineConfigSpec extends AnyFlatSpec with Matchers {

  "VotesPipelineConfig" should "derive from a full HOCON block including a non-empty congresses list" in {
    // Per P6.H5 the votes pipeline iterates over a list of congresses (resolved at startup from VOTES_CONGRESSES env,
    // config.pipeline.congresses, or a SELECT DISTINCT congress against bills). When the list is supplied via HOCON it
    // must round-trip through PureConfig as a `List[Int]`.
    val raw = """
      |pipeline {
      |  house {
      |    parallelism = 2
      |    page-delay = 3s
      |    lookback-days = 14
      |  }
      |  senate {
      |    base-url = "http://senate.example/LIS"
      |    parallelism = 1
      |    request-delay = 3s
      |    retry {
      |      max-retries = 3
      |      initial-backoff-ms = 10
      |      max-backoff-ms = 60000
      |      backoff-multiplier = 2.0
      |    }
      |  }
      |  congresses = [117, 118, 119]
      |}
      |""".stripMargin

    val cfg =
      ConfigSource.fromConfig(ConfigFactory.parseString(raw)).at("pipeline").loadOrThrow[VotesPipelineConfig]
    val _ = cfg.house.parallelism shouldBe 2
    val _ = cfg.house.pageDelay shouldBe 3.seconds
    val _ = cfg.house.lookbackDays shouldBe 14
    val _ = cfg.senate.baseUrl shouldBe "http://senate.example/LIS"
    val _ = cfg.senate.parallelism shouldBe 1
    val _ = cfg.senate.requestDelay shouldBe 3.seconds
    cfg.congresses shouldBe List(117, 118, 119)
  }

  it should "load with an empty congresses list when HOCON binds it to []" in {
    // application.conf ships `pipeline.congresses = []` — when no env override is supplied the pipeline falls back to
    // the bills-table-derived congresses. Verify HOCON round-trips an empty list cleanly.
    val raw = """
      |pipeline {
      |  house {
      |    parallelism = 1
      |    page-delay = 2s
      |    lookback-days = 7
      |  }
      |  senate {
      |    base-url = "https://www.senate.gov/legislative/LIS"
      |    parallelism = 1
      |    request-delay = 3s
      |    retry {
      |      max-retries = 3
      |      initial-backoff-ms = 10
      |      max-backoff-ms = 60000
      |      backoff-multiplier = 2.0
      |    }
      |  }
      |  congresses = []
      |}
      |""".stripMargin

    val cfg =
      ConfigSource.fromConfig(ConfigFactory.parseString(raw)).at("pipeline").loadOrThrow[VotesPipelineConfig]
    cfg.congresses shouldBe empty
  }

  // PureConfig's Scala 3 `derives ConfigReader` does NOT honor case-class default arguments — every field must be
  // present in HOCON or loading fails. Case-class defaults are still available for direct constructor usage from
  // Scala code (see the "case class accessors" test below) which is useful for wiring test fixtures and for places
  // that build the config programmatically rather than from HOCON.
  it should "fail to load when the house sub-block is omitted from HOCON" in {
    val raw = """
      |pipeline {
      |  senate {
      |    base-url = "https://www.senate.gov/legislative/LIS"
      |    parallelism = 1
      |    request-delay = 3s
      |    retry {
      |      max-retries = 3
      |      initial-backoff-ms = 10
      |      max-backoff-ms = 60000
      |      backoff-multiplier = 2.0
      |    }
      |  }
      |  congresses = []
      |}
      |""".stripMargin

    val attempt = ConfigSource.fromConfig(ConfigFactory.parseString(raw)).at("pipeline").load[VotesPipelineConfig]
    attempt.isLeft shouldBe true
  }

  it should "default congresses to Nil when constructed directly from Scala code" in {
    val cfg = VotesPipelineConfig(
      house = HouseVotesConfig(),
      senate = SenateVoteXmlConfig(),
    )
    cfg.congresses shouldBe Nil
  }

  it should "expose all fields via the case class accessors" in {
    val cfg = VotesPipelineConfig(
      house = HouseVotesConfig(parallelism = 4, pageDelay = 500.millis, lookbackDays = 30),
      senate = SenateVoteXmlConfig(parallelism = 2, requestDelay = 1.second),
      congresses = List(115, 116, 117, 118, 119),
    )
    val _ = cfg.house.parallelism shouldBe 4
    val _ = cfg.senate.parallelism shouldBe 2
    cfg.congresses shouldBe List(115, 116, 117, 118, 119)
  }

}
