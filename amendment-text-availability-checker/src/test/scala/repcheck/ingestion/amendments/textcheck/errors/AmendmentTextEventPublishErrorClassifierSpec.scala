package repcheck.ingestion.amendments.textcheck.errors

import java.io.IOException
import java.util.concurrent.TimeoutException

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import repcheck.pipeline.models.errors.ErrorClass

class AmendmentTextEventPublishErrorClassifierSpec extends AnyFlatSpec with Matchers {

  "AmendmentTextEventPublishErrorClassifier" should "classify IOException as Transient" in {
    AmendmentTextEventPublishErrorClassifier.classify(new IOException("network down")) shouldBe
      ErrorClass.Transient
  }

  it should "classify TimeoutException as Transient" in {
    AmendmentTextEventPublishErrorClassifier.classify(new TimeoutException("publish timeout")) shouldBe
      ErrorClass.Transient
  }

  it should "classify other Throwables as Systemic" in {
    val _ = AmendmentTextEventPublishErrorClassifier.classify(new RuntimeException("x")) shouldBe
      ErrorClass.Systemic
    AmendmentTextEventPublishErrorClassifier.classify(new IllegalStateException("y")) shouldBe
      ErrorClass.Systemic
  }

}
