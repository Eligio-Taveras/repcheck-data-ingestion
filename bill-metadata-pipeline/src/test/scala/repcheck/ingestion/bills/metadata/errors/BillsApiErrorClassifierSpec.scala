package repcheck.ingestion.bills.metadata.errors

import java.io.IOException
import java.net.{SocketException, SocketTimeoutException}
import java.util.concurrent.TimeoutException

import org.http4s.ember.core.EmberException

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.repcheck.utils.errors.ErrorClass

class BillsApiErrorClassifierSpec extends AnyFlatSpec with Matchers {

  "BillsApiErrorClassifier" should "classify HTTP 429 as Transient" in {
    BillsApiErrorClassifier.classify(BillsApiHttpError(429, "Rate limited")) shouldBe ErrorClass.Transient
  }

  it should "classify HTTP 500 as Transient" in {
    BillsApiErrorClassifier.classify(BillsApiHttpError(500, "Internal error")) shouldBe ErrorClass.Transient
  }

  it should "classify HTTP 502 as Transient" in {
    BillsApiErrorClassifier.classify(BillsApiHttpError(502, "Bad Gateway")) shouldBe ErrorClass.Transient
  }

  it should "classify HTTP 503 as Transient" in {
    BillsApiErrorClassifier.classify(BillsApiHttpError(503, "Service Unavailable")) shouldBe ErrorClass.Transient
  }

  it should "classify HTTP 504 as Transient" in {
    BillsApiErrorClassifier.classify(BillsApiHttpError(504, "Gateway Timeout")) shouldBe ErrorClass.Transient
  }

  it should "classify HTTP 401 as Systemic" in {
    BillsApiErrorClassifier.classify(BillsApiHttpError(401, "Unauthorized")) shouldBe ErrorClass.Systemic
  }

  it should "classify HTTP 403 as Systemic" in {
    BillsApiErrorClassifier.classify(BillsApiHttpError(403, "Forbidden")) shouldBe ErrorClass.Systemic
  }

  it should "classify HTTP 404 as Systemic" in {
    BillsApiErrorClassifier.classify(BillsApiHttpError(404, "Not Found")) shouldBe ErrorClass.Systemic
  }

  it should "classify non-BillsApiHttpError Throwables as Systemic" in {
    BillsApiErrorClassifier.classify(new RuntimeException("other")) shouldBe ErrorClass.Systemic
  }

  // Transport-level errors (added with the EOF classifier fix). These bypass the HTTP-status
  // path and are caught directly by the cause-chain walk.

  it should "classify EmberException.ReachedEndOfStream as Transient" in {
    BillsApiErrorClassifier.classify(EmberException.ReachedEndOfStream()) shouldBe ErrorClass.Transient
  }

  it should "classify SocketTimeoutException as Transient" in {
    BillsApiErrorClassifier.classify(new SocketTimeoutException("read timed out")) shouldBe ErrorClass.Transient
  }

  it should "classify SocketException as Transient" in {
    BillsApiErrorClassifier.classify(new SocketException("connection reset")) shouldBe ErrorClass.Transient
  }

  it should "classify TimeoutException as Transient" in {
    BillsApiErrorClassifier.classify(new TimeoutException("request exceeded 30s")) shouldBe ErrorClass.Transient
  }

  it should "classify IOException as Transient (catch-all for transport)" in {
    BillsApiErrorClassifier.classify(new IOException("broken pipe")) shouldBe ErrorClass.Transient
  }

  // Cause-chain walking. The classifier walks `getCause` so wrapped exceptions
  // (e.g. BillFetchFailed wrapping the underlying ember error) still resolve.

  it should "walk the cause chain to find an EOF wrapped in a generic RuntimeException" in {
    val wrapped = new RuntimeException("page fetch failed", EmberException.ReachedEndOfStream())
    BillsApiErrorClassifier.classify(wrapped) shouldBe ErrorClass.Transient
  }

  it should "walk the cause chain to find a SocketException wrapped in BillFetchFailed" in {
    val wrapped =
      BillFetchFailed("https://api.congress.gov/v3/bill", 0, "connection reset", new SocketException("reset"))
    BillsApiErrorClassifier.classify(wrapped) shouldBe ErrorClass.Transient
  }

  it should "walk the cause chain to find a transient HTTP error wrapped in BillFetchFailed" in {
    val wrapped =
      BillFetchFailed("https://api.congress.gov/v3/bill", 0, "rate limited", BillsApiHttpError(429, "Rate limited"))
    BillsApiErrorClassifier.classify(wrapped) shouldBe ErrorClass.Transient
  }

  it should "stop at the depth bound and classify Systemic when no transient cause is found" in {
    // Build a depth-12 cause chain of plain RuntimeExceptions (no transient-class anywhere).
    // The walker has a depth bound of 10; deeper-than-that chains terminate at Systemic
    // even if the very last layer were transient (which it isn't here).
    val deep = (1 to 12).foldLeft[Throwable](new RuntimeException("leaf")) {
      case (cause, idx) =>
        new RuntimeException(s"layer $idx", cause)
    }
    BillsApiErrorClassifier.classify(deep) shouldBe ErrorClass.Systemic
  }

  it should "classify a plain RuntimeException without cause as Systemic" in {
    BillsApiErrorClassifier.classify(new RuntimeException("no cause")) shouldBe ErrorClass.Systemic
  }

}
