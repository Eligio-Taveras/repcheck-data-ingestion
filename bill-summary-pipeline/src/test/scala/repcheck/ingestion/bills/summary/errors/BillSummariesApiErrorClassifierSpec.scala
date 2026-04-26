package repcheck.ingestion.bills.summary.errors

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import repcheck.pipeline.models.errors.ErrorClass

class BillSummariesApiErrorClassifierSpec extends AnyFlatSpec with Matchers {

  "BillSummariesApiErrorClassifier" should "classify 429 as Transient" in {
    BillSummariesApiErrorClassifier.classify(BillSummariesApiHttpError(429, "rate limit")) shouldBe ErrorClass.Transient
  }

  it should "classify 5xx as Transient" in {
    val _ =
      BillSummariesApiErrorClassifier.classify(BillSummariesApiHttpError(500, "server")) shouldBe ErrorClass.Transient
    val _ =
      BillSummariesApiErrorClassifier.classify(BillSummariesApiHttpError(502, "bad gw")) shouldBe ErrorClass.Transient
    val _ =
      BillSummariesApiErrorClassifier.classify(BillSummariesApiHttpError(503, "down")) shouldBe ErrorClass.Transient
    BillSummariesApiErrorClassifier.classify(BillSummariesApiHttpError(504, "timeout")) shouldBe ErrorClass.Transient
  }

  it should "classify 4xx (other than 429) as Systemic" in {
    val _ = BillSummariesApiErrorClassifier.classify(
      BillSummariesApiHttpError(400, "bad request")
    ) shouldBe ErrorClass.Systemic
    val _ = BillSummariesApiErrorClassifier.classify(
      BillSummariesApiHttpError(401, "unauthorized")
    ) shouldBe ErrorClass.Systemic
    val _ =
      BillSummariesApiErrorClassifier.classify(BillSummariesApiHttpError(403, "forbidden")) shouldBe ErrorClass.Systemic
    BillSummariesApiErrorClassifier.classify(BillSummariesApiHttpError(404, "not found")) shouldBe ErrorClass.Systemic
  }

  it should "classify non-HttpStatusError throwables as Systemic" in {
    BillSummariesApiErrorClassifier.classify(new RuntimeException("generic")) shouldBe ErrorClass.Systemic
  }

}
