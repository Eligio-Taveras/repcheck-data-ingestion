package com.repcheck.bills.text.config

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class BillTextPipelineConfigSpec extends AnyFlatSpec with Matchers {

  "BillTextPipelineConfig" should "hold parallelism setting" in {
    val config = BillTextPipelineConfig(parallelism = 4, downloadTimeoutSeconds = 60, maxContentBytes = 10485760L)
    config.parallelism shouldBe 4
  }

  it should "hold download timeout" in {
    val config = BillTextPipelineConfig(parallelism = 1, downloadTimeoutSeconds = 30, maxContentBytes = 10485760L)
    config.downloadTimeoutSeconds shouldBe 30
  }

  it should "hold max content bytes" in {
    val config = BillTextPipelineConfig(parallelism = 1, downloadTimeoutSeconds = 60, maxContentBytes = 5242880L)
    config.maxContentBytes shouldBe 5242880L
  }

}
