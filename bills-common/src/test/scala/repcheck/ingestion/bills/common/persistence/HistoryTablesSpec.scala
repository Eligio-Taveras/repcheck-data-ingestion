package repcheck.ingestion.bills.common.persistence

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class HistoryTablesSpec extends AnyFlatSpec with Matchers {

  "HistoryTables" should "have correct BillHistory table name" in {
    HistoryTables.BillHistory shouldBe "bill_history"
  }

  it should "have correct BillCosponsorHistory table name" in {
    HistoryTables.BillCosponsorHistory shouldBe "bill_cosponsor_history"
  }

  it should "have correct BillSubjectHistory table name" in {
    HistoryTables.BillSubjectHistory shouldBe "bill_subject_history"
  }

  it should "have non-empty BillHistory value" in {
    HistoryTables.BillHistory should not be empty
  }

  it should "have non-empty BillCosponsorHistory value" in {
    HistoryTables.BillCosponsorHistory should not be empty
  }

  it should "have non-empty BillSubjectHistory value" in {
    HistoryTables.BillSubjectHistory should not be empty
  }

}
