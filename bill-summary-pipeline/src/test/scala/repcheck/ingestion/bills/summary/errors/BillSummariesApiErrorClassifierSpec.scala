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

  it should "classify java.io.IOException as Transient (network drop / closed socket)" in {
    BillSummariesApiErrorClassifier.classify(new java.io.IOException("connection reset")) shouldBe ErrorClass.Transient
  }

  it should "classify java.net.SocketTimeoutException as Transient (subclass of IOException)" in {
    BillSummariesApiErrorClassifier.classify(
      new java.net.SocketTimeoutException("read timeout")
    ) shouldBe ErrorClass.Transient
  }

  it should "classify java.net.ConnectException as Transient (subclass of IOException)" in {
    BillSummariesApiErrorClassifier.classify(new java.net.ConnectException("refused")) shouldBe ErrorClass.Transient
  }

  it should "classify EmberException.ReachedEndOfStream as Transient (server closed connection mid-page)" in {
    val ember = new org.http4s.ember.core.EmberException.ReachedEndOfStream
    BillSummariesApiErrorClassifier.classify(ember) shouldBe ErrorClass.Transient
  }

  it should "walk the cause chain and classify wrapped IOException as Transient" in {
    // BillSummaryFetchFailed wraps the underlying network exception. The classifier walks
    // getCause to find the transient root.
    val rootCause = new java.io.IOException("connection reset by peer")
    val wrapped   = BillSummaryFetchFailed("https://example.com", 0, "Reached end of stream", rootCause)
    BillSummariesApiErrorClassifier.classify(wrapped) shouldBe ErrorClass.Transient
  }

  it should "walk the cause chain and classify wrapped EmberException as Transient" in {
    val rootCause = new org.http4s.ember.core.EmberException.ReachedEndOfStream
    val wrapped   = BillSummaryFetchFailed("https://example.com", 0, "Reached end of stream", rootCause)
    BillSummariesApiErrorClassifier.classify(wrapped) shouldBe ErrorClass.Transient
  }

  it should "stop walking the cause chain at HttpStatusError (avoid re-classifying as Transient via cause)" in {
    // HttpStatusError already has its own status-code-driven classification — if the root status
    // says 401, we return Systemic from the parent classify, NOT Transient via cause-walk.
    val httpErr = BillSummariesApiHttpError(401, "unauthorized")
    BillSummariesApiErrorClassifier.classify(httpErr) shouldBe ErrorClass.Systemic
  }

  it should "tolerate a self-referential cause without infinite-looping" in {
    // `Throwable.initCause(this)` throws IllegalArgumentException, so simulate the cycle by
    // overriding getCause() to return self. The classifier's `other.getCause != other` guard
    // breaks the cycle defensively.
    val loop = new RuntimeException("loop") {
      override def getCause: Throwable = this
    }
    BillSummariesApiErrorClassifier.classify(loop) shouldBe ErrorClass.Systemic
  }

}
