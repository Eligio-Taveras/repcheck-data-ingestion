package com.repcheck.bills.text.errors

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class BillNotFoundForTextSpec extends AnyFlatSpec with Matchers {

  "BillNotFoundForText" should "include natural key in message" in {
    val ex = BillNotFoundForText("118-HR-1")
    ex.getMessage should include("118-HR-1")
  }

  it should "indicate bill not found" in {
    val ex = BillNotFoundForText("118-S-42")
    ex.getMessage should include("not found")
  }

  it should "be an Exception" in {
    val ex = BillNotFoundForText("118-HR-1")
    ex shouldBe a[Exception]
  }

}
