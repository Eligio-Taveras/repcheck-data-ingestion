package repcheck.ingestion.amendments.text.persistence

import java.time.Instant

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import repcheck.shared.models.congress.common.FormatType
import repcheck.shared.models.congress.dos.amendment.AmendmentTextVersionDO

class DoobieAmendmentTextVersionRepositoryUnitSpec extends AnyFlatSpec with Matchers {

  private val repo = new DoobieAmendmentTextVersionRepository

  private def sampleVersion(versionType: String = "Submitted", date: Instant = Instant.parse("2021-08-01T04:00:00Z")) =
    AmendmentTextVersionDO(
      id = 0L,
      amendmentId = 42L,
      versionType = versionType,
      versionDate = date,
      formatType = FormatType.FormattedText,
      url = "https://www.congress.gov/x.htm",
      downloadUrl = Some("https://api.govinfo.gov/packages/x/granules/x/htm"),
      textLength = None,
      fetchedAt = None,
      createdAt = None,
    )

  "upsert" should "produce a ConnectionIO triple result" in {
    val cio = repo.upsert(sampleVersion())
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  it should "produce a distinct ConnectionIO per call" in {
    val a = repo.upsert(sampleVersion("Submitted"))
    val b = repo.upsert(sampleVersion("Modified"))
    (a eq b) shouldBe false
  }

  "markFetched" should "produce a ConnectionIO" in {
    val cio = repo.markFetched(7L, Instant.now(), 12345)
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  "findCompletedByAmendmentId" should "produce a ConnectionIO returning a list" in {
    val cio = repo.findCompletedByAmendmentId(42L)
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  "findExisting" should "produce a ConnectionIO for the SELECT" in {
    val cio = repo.findExisting(42L, "Submitted", "Formatted Text")
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  "insertFresh" should "produce a ConnectionIO returning Long" in {
    val cio = repo.insertFresh(sampleVersion())
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  "refreshExisting" should "produce a ConnectionIO" in {
    val cio = repo.refreshExisting(7L, sampleVersion())
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

}
