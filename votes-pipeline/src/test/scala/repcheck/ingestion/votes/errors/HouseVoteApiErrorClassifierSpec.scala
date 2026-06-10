package repcheck.ingestion.votes.errors

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.repcheck.utils.errors.ErrorClass

class HouseVoteApiErrorClassifierSpec extends AnyFlatSpec with Matchers {

  private val classifier = HouseVoteApiErrorClassifier

  "HouseVoteApiErrorClassifier" should "classify Congress.gov transient HTTP statuses (429/500/502/503/504) as Transient" in {
    val _ =
      classifier.classify(HouseVoteApiHttpError(statusCode = 429, body = "rate limited")) shouldBe ErrorClass.Transient
    val _ = classifier.classify(HouseVoteApiHttpError(statusCode = 500, body = "boom")) shouldBe ErrorClass.Transient
    val _ = classifier.classify(HouseVoteApiHttpError(statusCode = 502, body = "")) shouldBe ErrorClass.Transient
    val _ = classifier.classify(HouseVoteApiHttpError(statusCode = 503, body = "")) shouldBe ErrorClass.Transient
    classifier.classify(HouseVoteApiHttpError(statusCode = 504, body = "")) shouldBe ErrorClass.Transient
  }

  it should "classify 4xx (other than 429) and 200-299 as Systemic" in {
    val _ = classifier.classify(HouseVoteApiHttpError(statusCode = 400, body = "")) shouldBe ErrorClass.Systemic
    val _ = classifier.classify(HouseVoteApiHttpError(statusCode = 401, body = "")) shouldBe ErrorClass.Systemic
    val _ = classifier.classify(HouseVoteApiHttpError(statusCode = 403, body = "")) shouldBe ErrorClass.Systemic
    classifier.classify(HouseVoteApiHttpError(statusCode = 404, body = "")) shouldBe ErrorClass.Systemic
  }

  it should "classify http4s/ember 'Reached End Of Stream While Reading' as Transient (network-level)" in {
    val ex = new RuntimeException("Reached End Of Stream While Reading")
    classifier.classify(ex) shouldBe ErrorClass.Transient
  }

  it should "classify connection-reset / broken-pipe / connection-closed as Transient" in {
    val _ = classifier.classify(new RuntimeException("Connection reset by peer")) shouldBe ErrorClass.Transient
    val _ = classifier.classify(new RuntimeException("Broken pipe")) shouldBe ErrorClass.Transient
    classifier.classify(new RuntimeException("Connection closed")) shouldBe ErrorClass.Transient
  }

  it should "classify SocketTimeoutException + ConnectException by type" in {
    val _ = classifier.classify(new java.net.SocketTimeoutException("Read timed out")) shouldBe ErrorClass.Transient
    classifier.classify(new java.net.ConnectException("Connection refused")) shouldBe ErrorClass.Transient
  }

  it should "classify generic non-network non-status errors as Systemic" in {
    val _ = classifier.classify(new IllegalStateException("oops")) shouldBe ErrorClass.Systemic
    classifier.classify(new RuntimeException("decoding failed: not a number")) shouldBe ErrorClass.Systemic
  }

  it should "match transient-network signatures case-insensitively" in {
    val _ = classifier.classify(new RuntimeException("REACHED END OF STREAM")) shouldBe ErrorClass.Transient
    classifier.classify(new RuntimeException("Read Timed Out")) shouldBe ErrorClass.Transient
  }

  it should "tolerate a null message — must not NPE, falls back to type check" in {
    val noMsg: Throwable = new RuntimeException()
    classifier.classify(noMsg) shouldBe ErrorClass.Systemic
  }

}
