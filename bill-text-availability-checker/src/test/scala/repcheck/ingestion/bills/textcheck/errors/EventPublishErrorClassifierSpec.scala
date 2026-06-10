package repcheck.ingestion.bills.textcheck.errors

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.repcheck.utils.errors.ErrorClass

class EventPublishErrorClassifierSpec extends AnyFlatSpec with Matchers {

  "EventPublishErrorClassifier" should "classify IOException as Transient" in {
    EventPublishErrorClassifier.classify(new java.io.IOException("network")) shouldBe ErrorClass.Transient
  }

  it should "classify SocketTimeoutException as Transient" in {
    EventPublishErrorClassifier.classify(new java.net.SocketTimeoutException("timeout")) shouldBe ErrorClass.Transient
  }

  it should "classify ConnectException as Transient" in {
    EventPublishErrorClassifier.classify(new java.net.ConnectException("refused")) shouldBe ErrorClass.Transient
  }

  it should "classify TimeoutException as Transient" in {
    EventPublishErrorClassifier.classify(
      new java.util.concurrent.TimeoutException("timed out")
    ) shouldBe ErrorClass.Transient
  }

  it should "classify RuntimeException as Systemic" in {
    EventPublishErrorClassifier.classify(new RuntimeException("unexpected")) shouldBe ErrorClass.Systemic
  }

  it should "classify IllegalArgumentException as Systemic" in {
    EventPublishErrorClassifier.classify(new IllegalArgumentException("bad arg")) shouldBe ErrorClass.Systemic
  }

}
