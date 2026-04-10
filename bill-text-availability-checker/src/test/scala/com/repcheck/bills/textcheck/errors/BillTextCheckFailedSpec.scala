package com.repcheck.bills.textcheck.errors

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class BillTextCheckFailedSpec extends AnyFlatSpec with Matchers {

  private val cause = new RuntimeException("timeout")

  "BillTextCheckFailed" should "format message with billId and detail" in {
    val err = BillTextCheckFailed("118-HR-1", "API timeout", cause)
    err.getMessage shouldBe "Failed to check text availability for bill 118-HR-1: API timeout"
  }

  it should "preserve the cause" in {
    BillTextCheckFailed("118-HR-1", "API timeout", cause).getCause shouldBe cause
  }

  it should "expose billId field" in {
    BillTextCheckFailed("118-HR-1", "404 response", cause).billId shouldBe "118-HR-1"
  }

  it should "expose detail field" in {
    BillTextCheckFailed("118-HR-1", "404 response", cause).detail shouldBe "404 response"
  }

  it should "be a Throwable subclass" in {
    BillTextCheckFailed("x", "y", cause) shouldBe a[Exception]
  }

}
