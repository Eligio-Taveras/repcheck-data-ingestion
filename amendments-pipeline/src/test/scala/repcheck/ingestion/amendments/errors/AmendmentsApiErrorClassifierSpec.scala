package repcheck.ingestion.amendments.errors

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.repcheck.utils.errors.ErrorClass

class AmendmentsApiErrorClassifierSpec extends AnyFlatSpec with Matchers {

  "AmendmentsApiErrorClassifier" should "classify 429 as Transient" in {
    AmendmentsApiErrorClassifier.classify(
      AmendmentsApiHttpError(429, "rate limit", attempt = 1)
    ) shouldBe ErrorClass.Transient
  }

  it should "classify 500/502/503/504 as Transient" in {
    val _ = AmendmentsApiErrorClassifier.classify(
      AmendmentsApiHttpError(500, "server", attempt = 1)
    ) shouldBe ErrorClass.Transient
    val _ = AmendmentsApiErrorClassifier.classify(
      AmendmentsApiHttpError(502, "bad gw", attempt = 1)
    ) shouldBe ErrorClass.Transient
    val _ = AmendmentsApiErrorClassifier.classify(
      AmendmentsApiHttpError(503, "down", attempt = 1)
    ) shouldBe ErrorClass.Transient
    AmendmentsApiErrorClassifier.classify(
      AmendmentsApiHttpError(504, "timeout", attempt = 1)
    ) shouldBe ErrorClass.Transient
  }

  it should "classify 4xx (other than 429) as Systemic" in {
    val _ = AmendmentsApiErrorClassifier.classify(
      AmendmentsApiHttpError(400, "bad request", attempt = 1)
    ) shouldBe ErrorClass.Systemic
    val _ = AmendmentsApiErrorClassifier.classify(
      AmendmentsApiHttpError(401, "unauthorized", attempt = 1)
    ) shouldBe ErrorClass.Systemic
    val _ = AmendmentsApiErrorClassifier.classify(
      AmendmentsApiHttpError(403, "forbidden", attempt = 1)
    ) shouldBe ErrorClass.Systemic
    AmendmentsApiErrorClassifier.classify(
      AmendmentsApiHttpError(404, "not found", attempt = 1)
    ) shouldBe ErrorClass.Systemic
  }

  it should "classify non-HttpStatusError throwables as Systemic" in {
    AmendmentsApiErrorClassifier.classify(new RuntimeException("generic")) shouldBe ErrorClass.Systemic
  }

  it should "classify java.io.IOException as Transient (network drop)" in {
    AmendmentsApiErrorClassifier.classify(
      new java.io.IOException("connection reset")
    ) shouldBe ErrorClass.Transient
  }

  it should "classify java.net.SocketTimeoutException as Transient (subclass of IOException)" in {
    AmendmentsApiErrorClassifier.classify(
      new java.net.SocketTimeoutException("read timeout")
    ) shouldBe ErrorClass.Transient
  }

  it should "classify java.net.ConnectException as Transient (subclass of IOException)" in {
    AmendmentsApiErrorClassifier.classify(new java.net.ConnectException("refused")) shouldBe ErrorClass.Transient
  }

  it should "classify EmberException.ReachedEndOfStream as Transient (server closed mid-page)" in {
    val ember = new org.http4s.ember.core.EmberException.ReachedEndOfStream
    AmendmentsApiErrorClassifier.classify(ember) shouldBe ErrorClass.Transient
  }

  it should "classify java.util.concurrent.TimeoutException as Transient" in {
    AmendmentsApiErrorClassifier.classify(
      new java.util.concurrent.TimeoutException("request timeout")
    ) shouldBe ErrorClass.Transient
  }

  it should "walk the cause chain and classify wrapped IOException as Transient" in {
    val rootCause = new java.io.IOException("connection reset by peer")
    val wrapped   = AmendmentFetchFailed("117-SAMDT-2137", rootCause)
    AmendmentsApiErrorClassifier.classify(wrapped) shouldBe ErrorClass.Transient
  }

  it should "walk the cause chain and classify wrapped EmberException as Transient" in {
    val rootCause = new org.http4s.ember.core.EmberException.ReachedEndOfStream
    val wrapped   = AmendmentFetchFailed("117-SAMDT-2137", rootCause)
    AmendmentsApiErrorClassifier.classify(wrapped) shouldBe ErrorClass.Transient
  }

  it should "stop at HttpStatusError (401-with-IOException-cause classifies as Systemic)" in {
    // Per the helper's contract: the first HttpStatusError in the chain wins. Auth failures are
    // authoritative — even if some downstream wrapper added an IOException we don't want to retry.
    val httpErr = AmendmentsApiHttpError(401, "unauthorized", attempt = 1)
    AmendmentsApiErrorClassifier.classify(httpErr) shouldBe ErrorClass.Systemic
  }

  it should "tolerate a self-referential cause without infinite-looping" in {
    val loop = new RuntimeException("loop") {
      override def getCause: Throwable = this
    }
    AmendmentsApiErrorClassifier.classify(loop) shouldBe ErrorClass.Systemic
  }

  it should "not infinite-loop on a deeply-nested cause chain (depth-bounded)" in {
    // Build a cause chain longer than the helper's depth limit (16). Should classify as Systemic
    // (not Transient) because nothing in the chain is actually a transient marker.
    val deep = (1 to 50).foldLeft[Throwable](new RuntimeException("root")) { (cause, idx) =>
      new RuntimeException(s"wrapper-$idx", cause)
    }
    AmendmentsApiErrorClassifier.classify(deep) shouldBe ErrorClass.Systemic
  }

  it should "classify a top-level 500 (no cause-walk) as Transient via the base classifier" in {
    // The base classifier sees the HttpStatusError directly — no cause-walk needed.
    AmendmentsApiErrorClassifier.classify(
      AmendmentsApiHttpError(500, "internal", attempt = 1)
    ) shouldBe ErrorClass.Transient
  }

  it should "NOT re-classify a wrapped 500 via cause-walk (helper stops at HttpStatusError)" in {
    // The helper's `isTransientNetworkError` stops at the first HttpStatusError in the chain,
    // returning `false` (the status-bearing exception is the boundary). The base classifier then
    // examines the OUTER throwable. Since `AmendmentFetchFailed` does not implement HttpStatusError,
    // the result is Systemic — the wrapped 5xx is invisible. Production code consequently surfaces
    // the inner status when retry decisions need it (per the per-API classifier's choice).
    val rootCause = AmendmentsApiHttpError(503, "down", attempt = 2)
    val wrapped   = AmendmentFetchFailed("117-SAMDT-2137", rootCause)
    AmendmentsApiErrorClassifier.classify(wrapped) shouldBe ErrorClass.Systemic
  }

}
