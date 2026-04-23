package repcheck.ingestion.votes.xml

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * Pure unit tests for [[SenateVoteUrls]]. Pins the exact URL shapes senate.gov serves so the client and the converter
 * both produce the same strings — the converter uses these to populate `VoteDO.sourceDataUrl`, and the client uses them
 * to drive HTTP fetches.
 *
 * The URL patterns were verified against live senate.gov on 2026-04-22 for vote 119-1-648:
 *   - `https://www.senate.gov/legislative/LIS/roll_call_votes/vote1191/vote_119_1_00648.xml` → HTTP 200 with the real
 *     `<roll_call_vote>` body.
 *   - `https://www.senate.gov/legislative/LIS/roll_call_lists/vote_menu_119_1.xml` → HTTP 200 with the session index.
 *
 * The previous production pattern (`roll_call_lists/vote_menu_{c}_{s}/vote_...xml`) was WRONG — senate.gov 302s it to
 * `file_not_found.htm`. See commit history around this spec for the fix.
 */
class SenateVoteUrlsSpec extends AnyFlatSpec with Matchers {

  private val base = "https://www.senate.gov/legislative/LIS"

  "voteXmlUrl" should "assemble the per-vote URL with roll_call_votes/vote{c}{s}/ path and 5-digit zero-padded voteNumber" in {
    SenateVoteUrls.voteXmlUrl(base, 119, 1, 648) shouldBe
      "https://www.senate.gov/legislative/LIS/roll_call_votes/vote1191/vote_119_1_00648.xml"
  }

  it should "zero-pad a single-digit vote number to 5 digits" in {
    SenateVoteUrls.voteXmlUrl(base, 119, 1, 7) shouldBe
      "https://www.senate.gov/legislative/LIS/roll_call_votes/vote1191/vote_119_1_00007.xml"
  }

  it should "produce the correct URL for session 2 (no underscore between congress and session in the segment name)" in {
    SenateVoteUrls.voteXmlUrl(base, 118, 2, 500) shouldBe
      "https://www.senate.gov/legislative/LIS/roll_call_votes/vote1182/vote_118_2_00500.xml"
  }

  it should "handle a maximum 5-digit vote number without overflow" in {
    SenateVoteUrls.voteXmlUrl(base, 119, 1, 99999) shouldBe
      "https://www.senate.gov/legislative/LIS/roll_call_votes/vote1191/vote_119_1_99999.xml"
  }

  it should "respect a WireMock-style test base URL" in {
    SenateVoteUrls.voteXmlUrl("http://127.0.0.1:8080", 119, 1, 42) shouldBe
      "http://127.0.0.1:8080/roll_call_votes/vote1191/vote_119_1_00042.xml"
  }

  "voteIndexUrl" should "assemble the per-session index URL with roll_call_lists path" in {
    SenateVoteUrls.voteIndexUrl(base, 119, 1) shouldBe
      "https://www.senate.gov/legislative/LIS/roll_call_lists/vote_menu_119_1.xml"
  }

  it should "use underscore between congress and session in the menu filename" in {
    SenateVoteUrls.voteIndexUrl(base, 118, 2) shouldBe
      "https://www.senate.gov/legislative/LIS/roll_call_lists/vote_menu_118_2.xml"
  }

}
