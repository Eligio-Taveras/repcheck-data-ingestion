package repcheck.ingestion.amendments.textcheck.errors

import java.io.IOException

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.repcheck.utils.errors.ErrorClass

class AmendmentTextCheckErrorClassifierSpec extends AnyFlatSpec with Matchers {

  "AmendmentTextCheckErrorClassifier" should "classify 429 as Transient" in {
    AmendmentTextCheckErrorClassifier.classify(
      AmendmentTextCheckHttpError(429, "rate limited", 1)
    ) shouldBe ErrorClass.Transient
  }

  it should "classify 500/502/503/504 as Transient" in {
    val _ = AmendmentTextCheckErrorClassifier.classify(AmendmentTextCheckHttpError(500, "x", 1)) shouldBe
      ErrorClass.Transient
    val _ = AmendmentTextCheckErrorClassifier.classify(AmendmentTextCheckHttpError(502, "x", 1)) shouldBe
      ErrorClass.Transient
    val _ = AmendmentTextCheckErrorClassifier.classify(AmendmentTextCheckHttpError(503, "x", 1)) shouldBe
      ErrorClass.Transient
    AmendmentTextCheckErrorClassifier.classify(AmendmentTextCheckHttpError(504, "x", 1)) shouldBe
      ErrorClass.Transient
  }

  it should "classify 4xx (non-429) as Systemic" in {
    val _ = AmendmentTextCheckErrorClassifier.classify(AmendmentTextCheckHttpError(401, "x", 1)) shouldBe
      ErrorClass.Systemic
    val _ = AmendmentTextCheckErrorClassifier.classify(AmendmentTextCheckHttpError(403, "x", 1)) shouldBe
      ErrorClass.Systemic
    AmendmentTextCheckErrorClassifier.classify(AmendmentTextCheckHttpError(400, "x", 1)) shouldBe
      ErrorClass.Systemic
  }

  it should "classify a bare IOException as Transient via the network-aware wrapper" in {
    AmendmentTextCheckErrorClassifier.classify(new IOException("connection reset")) shouldBe
      ErrorClass.Transient
  }

  it should "classify an IOException-caused HttpStatusError by its status (Systemic for 401)" in {
    val ex = AmendmentTextCheckHttpError(401, "auth", 1)
    AmendmentTextCheckErrorClassifier.classify(ex) shouldBe ErrorClass.Systemic
  }

  it should "classify an unknown Throwable without an HttpStatusError as Systemic" in {
    AmendmentTextCheckErrorClassifier.classify(new RuntimeException("unknown")) shouldBe
      ErrorClass.Systemic
  }

}
