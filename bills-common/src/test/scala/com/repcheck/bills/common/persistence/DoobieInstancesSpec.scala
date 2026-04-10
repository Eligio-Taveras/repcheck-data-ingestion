package com.repcheck.bills.common.persistence

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class DoobieInstancesSpec extends AnyFlatSpec with Matchers {

  "parseFloatVector" should "parse a PostgreSQL vector string" in {
    val result = DoobieInstances.parseFloatVector("[1.0,2.5,3.14]")
    result shouldBe Array(1.0f, 2.5f, 3.14f)
  }

  it should "handle spaces between values" in {
    val result = DoobieInstances.parseFloatVector("[1.0, 2.5, 3.14]")
    result shouldBe Array(1.0f, 2.5f, 3.14f)
  }

  it should "return empty array for empty brackets" in {
    val result = DoobieInstances.parseFloatVector("[]")
    result shouldBe Array.empty[Float]
  }

  it should "parse a single-element vector" in {
    val result = DoobieInstances.parseFloatVector("[42.0]")
    result shouldBe Array(42.0f)
  }

  it should "parse negative values" in {
    val result = DoobieInstances.parseFloatVector("[-1.5,2.0,-3.7]")
    result shouldBe Array(-1.5f, 2.0f, -3.7f)
  }

  "formatFloatVector" should "format an array to PostgreSQL vector string" in {
    val result = DoobieInstances.formatFloatVector(Array(1.0f, 2.5f, 3.14f))
    result shouldBe "[1.0,2.5,3.14]"
  }

  it should "format an empty array" in {
    val result = DoobieInstances.formatFloatVector(Array.empty[Float])
    result shouldBe "[]"
  }

  it should "format a single-element array" in {
    val result = DoobieInstances.formatFloatVector(Array(42.0f))
    result shouldBe "[42.0]"
  }

  "round-trip" should "preserve values through format then parse" in {
    val original     = Array(1.0f, -2.5f, 0.0f, 3.14f)
    val roundTripped = DoobieInstances.parseFloatVector(DoobieInstances.formatFloatVector(original))
    roundTripped shouldBe original
  }

  "floatArrayGet" should "be a resolvable implicit instance" in {
    DoobieInstances.floatArrayGet.toString should not be empty
  }

  "floatArrayPut" should "be a resolvable implicit instance" in {
    DoobieInstances.floatArrayPut.toString should not be empty
  }

}
