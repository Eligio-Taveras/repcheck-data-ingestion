package repcheck.ingestion.bills.metadata.errors

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class BillProcessingFailedSpec extends AnyFlatSpec with Matchers {

  "BillProcessingFailed" should "include bill ID in message" in {
    val ex = BillProcessingFailed("118-HR-1", "conversion error")
    ex.getMessage should include("118-HR-1")
  }

  it should "include detail in message" in {
    val ex = BillProcessingFailed("118-HR-1", "title must not be empty")
    ex.getMessage should include("title must not be empty")
  }

}
