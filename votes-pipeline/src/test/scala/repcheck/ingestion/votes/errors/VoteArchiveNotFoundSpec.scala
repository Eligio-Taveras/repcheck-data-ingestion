package repcheck.ingestion.votes.errors

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * Unit spec for [[VoteArchiveNotFound]]. Keeps the exception class at 100% coverage and pins the message shape so
 * future refactors don't silently break log / dashboard parsers that key on it.
 */
class VoteArchiveNotFoundSpec extends AnyFlatSpec with Matchers {

  "VoteArchiveNotFound" should "render a message that includes the vote id" in {
    val ex = VoteArchiveNotFound(42L)
    ex.getMessage should include("42")
  }

  it should "name the precondition concern in the message" in {
    val ex = VoteArchiveNotFound(99L)
    ex.getMessage should include("Cannot archive")
  }

  it should "be a subclass of Exception" in {
    VoteArchiveNotFound(0L) shouldBe a[Exception]
  }

  it should "preserve the voteId via the case-class field" in {
    VoteArchiveNotFound(7L).voteId shouldBe 7L
  }

}
