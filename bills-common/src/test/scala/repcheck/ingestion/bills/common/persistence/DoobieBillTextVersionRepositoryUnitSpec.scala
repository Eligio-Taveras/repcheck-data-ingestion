package repcheck.ingestion.bills.common.persistence

import java.time.{Instant, LocalDate}

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import repcheck.shared.models.congress.common.FormatType
import repcheck.shared.models.congress.dos.bill.BillTextVersionDO

class DoobieBillTextVersionRepositoryUnitSpec extends AnyFlatSpec with Matchers {

  private val repo = new DoobieBillTextVersionRepository

  private val sampleVersion = BillTextVersionDO(
    id = 0L,
    billId = 1L,
    versionCode = "IH",
    versionType = "Introduced in House",
    versionDate = Some(LocalDate.parse("2024-01-15")),
    formatType = Some(FormatType.FormattedXml),
    url = Some("http://example.com/text"),
    fetchedAt = Some(Instant.parse("2024-01-15T12:00:00Z")),
    createdAt = None,
  )

  "insertVersion" should "produce a ConnectionIO for the insert" in {
    val cio = repo.insertVersion(sampleVersion)
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  it should "handle version with no optional fields" in {
    val minimal = sampleVersion.copy(
      versionDate = None,
      formatType = None,
      url = None,
      fetchedAt = None,
    )
    val cio = repo.insertVersion(minimal)
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  "findByBillId" should "produce a ConnectionIO for the query" in {
    val cio = repo.findByBillId(1L)
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  "findLatestByBillId" should "produce a ConnectionIO for the query" in {
    val cio = repo.findLatestByBillId(1L)
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  "storeAndUpdateBill" should "produce a ConnectionIO for the composite operation" in {
    val cio = repo.storeAndUpdateBill(sampleVersion)
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  "buildBillTextFieldsUpdate" should "produce a ConnectionIO for the bill fields update" in {
    val cio = repo.buildBillTextFieldsUpdate(sampleVersion, 42L)
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

}
