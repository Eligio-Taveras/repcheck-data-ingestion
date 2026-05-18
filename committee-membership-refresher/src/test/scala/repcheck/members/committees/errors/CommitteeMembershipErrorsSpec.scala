package repcheck.members.committees.errors

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class CommitteeMembershipErrorsSpec extends AnyFlatSpec with Matchers {

  "CommitteeFetchFailed" should "format message with detail" in {
    val error = CommitteeFetchFailed("connection refused")
    error.getMessage shouldBe "Failed to fetch committee data: connection refused"
  }

  it should "chain cause when provided" in {
    val rootCause = new RuntimeException("root")
    val error     = CommitteeFetchFailed("wrapped", Some(rootCause))
    val _         = error.cause shouldBe Some(rootCause)
    error.getCause shouldBe rootCause
  }

  it should "default cause to None" in {
    CommitteeFetchFailed("test").cause shouldBe None
  }

  "CommitteeUpsertFailed" should "include committee code in message" in {
    val error = CommitteeUpsertFailed("SSFI00", "constraint violation")
    error.getMessage shouldBe "Failed to upsert committee SSFI00: constraint violation"
  }

  it should "chain cause when provided" in {
    val rootCause = new RuntimeException("db error")
    val error     = CommitteeUpsertFailed("RU00", "upsert failed", Some(rootCause))
    error.getCause shouldBe rootCause
  }

  "CommitteeMemberUpsertFailed" should "include committee code and member id" in {
    val error = CommitteeMemberUpsertFailed("SSFI00", 42L, "duplicate key")
    error.getMessage shouldBe "Failed to upsert committee member SSFI00/member=42: duplicate key"
  }

  "CommitteeApiHttpError" should "include status code and body" in {
    val error = CommitteeApiHttpError(500, "Internal Server Error")
    error.getMessage shouldBe "Congress.gov committee API returned HTTP 500: Internal Server Error"
  }

  "SenateCommitteeSoft404" should "include committee code" in {
    val error = SenateCommitteeSoft404("SSXX00")
    error.getMessage shouldBe "Senate committee XML returned HTML (soft-404) for SSXX00"
  }

  "MemberResolutionFailed" should "include bioguide and detail" in {
    val error = MemberResolutionFailed("B000001", "not in members table")
    error.getMessage shouldBe "Failed to resolve member B000001: not in members table"
  }

  "All error types" should "be instances of Exception" in {
    val _ = CommitteeFetchFailed("test") shouldBe a[Exception]
    val _ = CommitteeUpsertFailed("X", "y") shouldBe a[Exception]
    val _ = CommitteeMemberUpsertFailed("X", 1L, "y") shouldBe a[Exception]
    val _ = CommitteeApiHttpError(400, "bad") shouldBe a[Exception]
    val _ = SenateCommitteeSoft404("X") shouldBe a[Exception]
    MemberResolutionFailed("B", "d") shouldBe a[Exception]
  }

}
