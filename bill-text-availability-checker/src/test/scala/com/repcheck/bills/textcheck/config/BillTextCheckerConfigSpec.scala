package com.repcheck.bills.textcheck.config

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class BillTextCheckerConfigSpec extends AnyFlatSpec with Matchers {

  "BillTextCheckerConfig" should "hold parallelism setting" in {
    val config = BillTextCheckerConfig(parallelism = 4)
    config.parallelism shouldBe 4
  }

  it should "allow single-threaded parallelism" in {
    val config = BillTextCheckerConfig(parallelism = 1)
    config.parallelism shouldBe 1
  }

}
