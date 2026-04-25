package repcheck.ingestion.bills.common.persistence

import cats.effect.IO
import cats.effect.unsafe.implicits.global

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import repcheck.shared.models.congress.common.FormatType
import repcheck.shared.models.congress.dos.bill.BillTextVersionDO

class BillTextVersionRepositorySpec extends AnyFlatSpec with Matchers {

  private val stubRepo: BillTextVersionRepository[IO] = new BillTextVersionRepository[IO] {

    override def insertVersion(version: BillTextVersionDO): IO[Long] = IO.pure(1L)

    override def findByBillId(billId: Long): IO[List[BillTextVersionDO]] = IO.pure(List.empty)

    override def findLatestByBillId(billId: Long): IO[Option[BillTextVersionDO]] = IO.pure(None)

    override def storeAndUpdateBill(version: BillTextVersionDO): IO[Long] = IO.pure(1L)
  }

  private val sampleVersion = BillTextVersionDO(
    id = 0L,
    billId = 1L,
    versionCode = "IH",
    versionType = "Introduced in House",
    versionDate = None,
    formatType = Some(FormatType.FormattedXml),
    url = Some("https://example.com/text"),
    fetchedAt = None,
    createdAt = None,
  )

  "BillTextVersionRepository" should "compile with a stub implementation" in {
    stubRepo.toString should not be empty
  }

  it should "return an ID from insertVersion for a stub" in {
    stubRepo.insertVersion(sampleVersion).unsafeRunSync() shouldBe 1L
  }

  it should "return empty list from findByBillId for a stub" in {
    stubRepo.findByBillId(1L).unsafeRunSync() shouldBe List.empty
  }

  it should "return None from findLatestByBillId for a stub" in {
    stubRepo.findLatestByBillId(1L).unsafeRunSync() shouldBe None
  }

  it should "return an ID from storeAndUpdateBill for a stub" in {
    stubRepo.storeAndUpdateBill(sampleVersion).unsafeRunSync() shouldBe 1L
  }

}
