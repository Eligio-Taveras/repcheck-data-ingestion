package repcheck.ingestion.bills.textcheck.errors

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import repcheck.pipeline.models.errors.ErrorClass

class BillTextApiErrorClassifierSpec extends AnyFlatSpec with Matchers {

  "BillTextApiErrorClassifier" should "classify HTTP 429 as Transient" in {
    BillTextApiErrorClassifier.classify(BillTextApiHttpError(429, "Rate limited")) shouldBe ErrorClass.Transient
  }

  it should "classify HTTP 500 as Transient" in {
    BillTextApiErrorClassifier.classify(BillTextApiHttpError(500, "Internal error")) shouldBe ErrorClass.Transient
  }

  it should "classify HTTP 502 as Transient" in {
    BillTextApiErrorClassifier.classify(BillTextApiHttpError(502, "Bad Gateway")) shouldBe ErrorClass.Transient
  }

  it should "classify HTTP 503 as Transient" in {
    BillTextApiErrorClassifier.classify(BillTextApiHttpError(503, "Service Unavailable")) shouldBe ErrorClass.Transient
  }

  it should "classify HTTP 504 as Transient" in {
    BillTextApiErrorClassifier.classify(BillTextApiHttpError(504, "Gateway Timeout")) shouldBe ErrorClass.Transient
  }

  it should "classify HTTP 401 as Systemic" in {
    BillTextApiErrorClassifier.classify(BillTextApiHttpError(401, "Unauthorized")) shouldBe ErrorClass.Systemic
  }

  it should "classify HTTP 403 as Systemic" in {
    BillTextApiErrorClassifier.classify(BillTextApiHttpError(403, "Forbidden")) shouldBe ErrorClass.Systemic
  }

  it should "classify HTTP 404 as Systemic" in {
    BillTextApiErrorClassifier.classify(BillTextApiHttpError(404, "Not Found")) shouldBe ErrorClass.Systemic
  }

  it should "classify non-BillTextApiHttpError Throwables as Systemic" in {
    BillTextApiErrorClassifier.classify(new RuntimeException("other")) shouldBe ErrorClass.Systemic
  }

}
