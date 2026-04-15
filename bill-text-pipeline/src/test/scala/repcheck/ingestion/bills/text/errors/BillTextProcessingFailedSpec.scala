package repcheck.ingestion.bills.text.errors

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class BillTextProcessingFailedSpec extends AnyFlatSpec with Matchers {

  "BillTextProcessingFailed" should "include bill ID in message" in {
    val ex = BillTextProcessingFailed("118-HR-1", "download failed")
    ex.getMessage should include("118-HR-1")
  }

  it should "include detail in message" in {
    val ex = BillTextProcessingFailed("118-HR-1", "content exceeds maximum size")
    ex.getMessage should include("content exceeds maximum size")
  }

}
