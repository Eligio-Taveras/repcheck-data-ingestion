package com.repcheck.bills.metadata.config

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class BillMetadataConfigSpec extends AnyFlatSpec with Matchers {

  "BillMetadataConfig" should "hold lookbackDays and parallelism" in {
    val config = BillMetadataConfig(lookbackDays = 30, parallelism = 4)
    val _      = config.lookbackDays shouldBe 30
    config.parallelism shouldBe 4
  }

  it should "allow single-threaded parallelism" in {
    val config = BillMetadataConfig(lookbackDays = 7, parallelism = 1)
    config.parallelism shouldBe 1
  }

}
