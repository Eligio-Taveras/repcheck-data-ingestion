package repcheck.ingestion.votes.errors

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import repcheck.pipeline.models.errors.ErrorClass

class SenateVoteXmlHttpErrorSpec extends AnyFlatSpec with Matchers {

  "SenateVoteXmlHttpError" should "surface the HTTP status code via HttpStatusError" in {
    val err = SenateVoteXmlHttpError(429, "rate limited")
    err.statusCode shouldBe 429
  }

  it should "render the body and status in the message" in {
    val err = SenateVoteXmlHttpError(500, "internal error")
    val _   = err.getMessage should include("500")
    err.getMessage should include("internal error")
  }

  "SenateVoteXmlErrorClassifier" should "classify 429 as Transient" in {
    val err = SenateVoteXmlHttpError(429, "rate limited")
    SenateVoteXmlErrorClassifier.classify(err) shouldBe ErrorClass.Transient
  }

  it should "classify 503 as Transient" in {
    SenateVoteXmlErrorClassifier.classify(SenateVoteXmlHttpError(503, "busy")) shouldBe ErrorClass.Transient
  }

  it should "classify 500 as Transient" in {
    SenateVoteXmlErrorClassifier.classify(SenateVoteXmlHttpError(500, "broke")) shouldBe ErrorClass.Transient
  }

  it should "classify 400 as Systemic" in {
    SenateVoteXmlErrorClassifier.classify(SenateVoteXmlHttpError(400, "bad")) shouldBe ErrorClass.Systemic
  }

  it should "classify 404 as Systemic" in {
    SenateVoteXmlErrorClassifier.classify(SenateVoteXmlHttpError(404, "missing")) shouldBe ErrorClass.Systemic
  }

}
