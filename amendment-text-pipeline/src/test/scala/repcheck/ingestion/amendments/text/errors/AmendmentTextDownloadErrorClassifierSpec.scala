package repcheck.ingestion.amendments.text.errors

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import repcheck.pipeline.models.errors.ErrorClass

class AmendmentTextDownloadErrorClassifierSpec extends AnyFlatSpec with Matchers {

  "classify" should "route 429 to Transient" in {
    AmendmentTextDownloadErrorClassifier.classify(AmendmentTextDownloadHttpError(429, "rate limit")) shouldBe
      ErrorClass.Transient
  }

  it should "route 500/502/503/504 to Transient" in {
    val _ = AmendmentTextDownloadErrorClassifier.classify(
      AmendmentTextDownloadHttpError(500, "")
    ) shouldBe ErrorClass.Transient
    val _ = AmendmentTextDownloadErrorClassifier.classify(
      AmendmentTextDownloadHttpError(502, "")
    ) shouldBe ErrorClass.Transient
    val _ = AmendmentTextDownloadErrorClassifier.classify(
      AmendmentTextDownloadHttpError(503, "")
    ) shouldBe ErrorClass.Transient
    AmendmentTextDownloadErrorClassifier.classify(AmendmentTextDownloadHttpError(504, "")) shouldBe ErrorClass.Transient
  }

  it should "route 401 (invalid api_key) to Systemic" in {
    AmendmentTextDownloadErrorClassifier.classify(AmendmentTextDownloadHttpError(401, "Unauthorized")) shouldBe
      ErrorClass.Systemic
  }

  it should "route 403 to Systemic" in {
    AmendmentTextDownloadErrorClassifier.classify(AmendmentTextDownloadHttpError(403, "Forbidden")) shouldBe
      ErrorClass.Systemic
  }

  it should "route generic IOException to Transient" in {
    AmendmentTextDownloadErrorClassifier.classify(new java.io.IOException("connection reset")) shouldBe
      ErrorClass.Transient
  }

  it should "route ConnectException (subclass of IOException) to Transient" in {
    AmendmentTextDownloadErrorClassifier.classify(new java.net.ConnectException("refused")) shouldBe
      ErrorClass.Transient
  }

  it should "route SocketTimeoutException (subclass of IOException) to Transient" in {
    AmendmentTextDownloadErrorClassifier.classify(new java.net.SocketTimeoutException("timeout")) shouldBe
      ErrorClass.Transient
  }

  it should "walk the cause chain to find a transient network exception" in {
    val deepCause = new java.io.IOException("network glitch")
    val wrapper   = new RuntimeException("wrapper", deepCause)
    AmendmentTextDownloadErrorClassifier.classify(wrapper) shouldBe ErrorClass.Transient
  }

  it should "route a runtime error with no transient cause to Systemic" in {
    AmendmentTextDownloadErrorClassifier.classify(new RuntimeException("nothing transient here")) shouldBe
      ErrorClass.Systemic
  }

  it should "not infinite-loop on a self-referencing cause" in {
    val selfRef = new Throwable("self") { override def getCause: Throwable = this }
    AmendmentTextDownloadErrorClassifier.classify(selfRef) shouldBe ErrorClass.Systemic
  }

  it should "stop walking after 16 levels of cause-chain depth" in {
    // Build a 20-deep cause chain of plain RuntimeException; the classifier should give up at the depth limit
    // and return Systemic rather than recursing forever.
    val deepest = new RuntimeException("deepest")
    val chain   = (1 to 20).foldLeft(deepest: Throwable) { case (acc, _) => new RuntimeException("layer", acc) }
    AmendmentTextDownloadErrorClassifier.classify(chain) shouldBe ErrorClass.Systemic
  }

  it should "treat a null in the chain safely" in {
    // Reach the null guard via a Throwable whose getCause returns null (the JVM default for unset cause).
    val noCause = new RuntimeException("no cause set")
    AmendmentTextDownloadErrorClassifier.isTransientNetworkError(noCause) shouldBe false
  }

}
