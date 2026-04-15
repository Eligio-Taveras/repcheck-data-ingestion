package repcheck.members.lismapping.repository

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class UpsertResultSpec extends AnyFlatSpec with Matchers {

  "UpsertResult.Inserted" should "be an instance of UpsertResult" in {
    val result: UpsertResult = UpsertResult.Inserted
    result shouldBe a[UpsertResult]
  }

  "UpsertResult.Updated" should "be an instance of UpsertResult" in {
    val result: UpsertResult = UpsertResult.Updated
    result shouldBe a[UpsertResult]
  }

  "UpsertResult" should "distinguish Inserted from Updated" in {
    val inserted: UpsertResult = UpsertResult.Inserted
    val updated: UpsertResult  = UpsertResult.Updated
    inserted should not be updated
  }

  it should "support pattern matching" in {
    val result: UpsertResult = UpsertResult.Inserted
    val label = result match {
      case UpsertResult.Inserted => "inserted"
      case UpsertResult.Updated  => "updated"
    }
    label shouldBe "inserted"
  }

}
