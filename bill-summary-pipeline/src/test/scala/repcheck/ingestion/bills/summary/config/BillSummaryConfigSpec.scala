package repcheck.ingestion.bills.summary.config

import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory

import pureconfig.ConfigSource

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class BillSummaryConfigSpec extends AnyFlatSpec with Matchers {

  "BillSummaryConfig" should "load every field from a HOCON block" in {
    val raw = ConfigFactory.parseString(
      """
        |initial-lookback-days = 3650
        |watermark-buffer = 5m
        |congresses = [114, 115, 116, 117, 118, 119]
        |step-name = "bill-summary-pipeline"
        |""".stripMargin
    )

    val cfg = ConfigSource.fromConfig(raw).loadOrThrow[BillSummaryConfig]
    val _   = cfg.initialLookbackDays shouldBe 3650
    val _   = cfg.watermarkBuffer shouldBe 5.minutes
    val _   = cfg.congresses shouldBe List(114, 115, 116, 117, 118, 119)
    cfg.stepName shouldBe "bill-summary-pipeline"
  }

  it should "support a single-congress steady-state config" in {
    val raw = ConfigFactory.parseString(
      """
        |initial-lookback-days = 30
        |watermark-buffer = 30s
        |congresses = [119]
        |step-name = "bill-summary"
        |""".stripMargin
    )

    val cfg = ConfigSource.fromConfig(raw).loadOrThrow[BillSummaryConfig]
    cfg.congresses shouldBe List(119)
  }

}
