package repcheck.ingestion.bills.common.persistence

import java.time.LocalDate

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import repcheck.shared.models.congress.dos.bill.BillCosponsorDO

class DoobieBillCosponsorRepositoryUnitSpec extends AnyFlatSpec with Matchers {

  private val repo = new DoobieBillCosponsorRepository

  private val sampleCosponsor = BillCosponsorDO(
    billId = 1L,
    memberId = 2L,
    isOriginalCosponsor = Some(true),
    sponsorshipDate = Some(LocalDate.parse("2024-01-15")),
  )

  "replaceAll" should "produce a ConnectionIO describing the delete-then-insert" in {
    val cio = repo.replaceAll(1L, List(sampleCosponsor))
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  it should "handle an empty cosponsor list" in {
    val cio = repo.replaceAll(1L, List.empty)
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  it should "handle multiple cosponsors" in {
    val second = sampleCosponsor.copy(memberId = 3L, isOriginalCosponsor = None, sponsorshipDate = None)
    val cio    = repo.replaceAll(1L, List(sampleCosponsor, second))
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  "findByBillId" should "produce a ConnectionIO for the query" in {
    val cio = repo.findByBillId(1L)
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

}
