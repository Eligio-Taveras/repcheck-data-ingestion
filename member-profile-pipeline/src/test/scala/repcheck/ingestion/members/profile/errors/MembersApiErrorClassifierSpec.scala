package repcheck.ingestion.members.profile.errors

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import repcheck.pipeline.models.errors.ErrorClass

class MembersApiErrorClassifierSpec extends AnyFlatSpec with Matchers {

  "MembersApiErrorClassifier" should "classify HTTP 429 as Transient" in {
    MembersApiErrorClassifier.classify(MembersApiHttpError(429, "Rate limited")) shouldBe ErrorClass.Transient
  }

  it should "classify HTTP 500 as Transient" in {
    MembersApiErrorClassifier.classify(MembersApiHttpError(500, "Internal error")) shouldBe ErrorClass.Transient
  }

  it should "classify HTTP 502 as Transient" in {
    MembersApiErrorClassifier.classify(MembersApiHttpError(502, "Bad Gateway")) shouldBe ErrorClass.Transient
  }

  it should "classify HTTP 503 as Transient" in {
    MembersApiErrorClassifier.classify(MembersApiHttpError(503, "Service Unavailable")) shouldBe ErrorClass.Transient
  }

  it should "classify HTTP 504 as Transient" in {
    MembersApiErrorClassifier.classify(MembersApiHttpError(504, "Gateway Timeout")) shouldBe ErrorClass.Transient
  }

  it should "classify HTTP 401 as Systemic" in {
    MembersApiErrorClassifier.classify(MembersApiHttpError(401, "Unauthorized")) shouldBe ErrorClass.Systemic
  }

  it should "classify HTTP 403 as Systemic" in {
    MembersApiErrorClassifier.classify(MembersApiHttpError(403, "Forbidden")) shouldBe ErrorClass.Systemic
  }

  it should "classify HTTP 404 as Systemic" in {
    MembersApiErrorClassifier.classify(MembersApiHttpError(404, "Not Found")) shouldBe ErrorClass.Systemic
  }

  it should "classify non-MembersApiHttpError Throwables as Systemic" in {
    MembersApiErrorClassifier.classify(new RuntimeException("other")) shouldBe ErrorClass.Systemic
  }

}
