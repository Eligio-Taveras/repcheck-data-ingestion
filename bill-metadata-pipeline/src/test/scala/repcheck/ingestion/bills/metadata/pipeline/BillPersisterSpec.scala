package repcheck.ingestion.bills.metadata.pipeline

import cats.effect.IO
import cats.effect.unsafe.implicits.global

import doobie._

import org.mockito.ArgumentMatchers.{any, anyString}
import org.mockito.Mockito.{never, times, verify, when}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar
import repcheck.ingestion.bills.common.persistence.{
  BillCosponsorRepository,
  BillHistoryArchiver,
  BillRepository,
  BillSubjectRepository,
}
import repcheck.shared.models.congress.common.BillType
import repcheck.shared.models.congress.dos.bill.{BillCosponsorDO, BillDO, BillSubjectDO}

class BillPersisterSpec extends AnyFlatSpec with Matchers with MockitoSugar {

  private val testXa: Transactor[IO] = Transactor.fromDriverManager[IO](
    driver = "org.h2.Driver",
    url = "jdbc:h2:mem:billpersister;DB_CLOSE_DELAY=-1",
    user = "",
    password = "",
    logHandler = None,
  )

  private val baseBillDO = BillDO(
    billId = 1L,
    naturalKey = "118-HR-1",
    congress = 118,
    billType = BillType.HR,
    number = "1",
    title = "Test",
    originChamber = None,
    originChamberCode = None,
    introducedDate = None,
    policyArea = None,
    latestActionDate = None,
    latestActionText = None,
    constitutionalAuthorityText = None,
    sponsorMemberId = None,
    textUrl = None,
    textFormat = None,
    textVersionType = None,
    textDate = None,
    textContent = None,
    summaryText = None,
    summaryActionDesc = None,
    summaryActionDate = None,
    updateDate = None,
    updateDateIncludingText = None,
    legislationUrl = None,
    apiUrl = None,
    createdAt = None,
    updatedAt = None,
    latestTextVersionId = None,
  )

  private def makePersister(
    billRepo: BillRepository[ConnectionIO],
    cosponsorRepo: BillCosponsorRepository[ConnectionIO],
    subjectRepo: BillSubjectRepository[ConnectionIO],
    historyArchiver: BillHistoryArchiver[ConnectionIO],
  ): BillPersister[IO] =
    new BillPersister[IO](billRepo, cosponsorRepo, subjectRepo, historyArchiver, testXa)

  "persistBill" should "archive history before upsert for existing bills" in {
    val billRepo        = mock[BillRepository[ConnectionIO]]
    val cosponsorRepo   = mock[BillCosponsorRepository[ConnectionIO]]
    val subjectRepo     = mock[BillSubjectRepository[ConnectionIO]]
    val historyArchiver = mock[BillHistoryArchiver[ConnectionIO]]

    when(historyArchiver.archiveBill(anyString())).thenReturn(doobie.free.connection.pure(1L))
    when(billRepo.upsert(any[BillDO])).thenReturn(doobie.free.connection.pure(42L))
    when(cosponsorRepo.replaceAll(any[Long], any[List[BillCosponsorDO]])).thenReturn(doobie.free.connection.pure(()))
    when(subjectRepo.replaceAll(any[Long], any[List[BillSubjectDO]])).thenReturn(doobie.free.connection.pure(()))

    val persister = makePersister(billRepo, cosponsorRepo, subjectRepo, historyArchiver)
    persister.persistBill(baseBillDO, List.empty, List.empty, "118-HR-1", isNew = false).unsafeRunSync()

    verify(historyArchiver, times(1)).archiveBill("118-HR-1")
  }

  it should "skip archive for new bills" in {
    val billRepo        = mock[BillRepository[ConnectionIO]]
    val cosponsorRepo   = mock[BillCosponsorRepository[ConnectionIO]]
    val subjectRepo     = mock[BillSubjectRepository[ConnectionIO]]
    val historyArchiver = mock[BillHistoryArchiver[ConnectionIO]]

    when(billRepo.upsert(any[BillDO])).thenReturn(doobie.free.connection.pure(42L))
    when(cosponsorRepo.replaceAll(any[Long], any[List[BillCosponsorDO]])).thenReturn(doobie.free.connection.pure(()))
    when(subjectRepo.replaceAll(any[Long], any[List[BillSubjectDO]])).thenReturn(doobie.free.connection.pure(()))

    val persister = makePersister(billRepo, cosponsorRepo, subjectRepo, historyArchiver)
    persister.persistBill(baseBillDO, List.empty, List.empty, "118-HR-1", isNew = true).unsafeRunSync()

    verify(historyArchiver, never()).archiveBill(anyString())
  }

  it should "set billId on cosponsors from upsert result" in {
    val billRepo        = mock[BillRepository[ConnectionIO]]
    val cosponsorRepo   = mock[BillCosponsorRepository[ConnectionIO]]
    val subjectRepo     = mock[BillSubjectRepository[ConnectionIO]]
    val historyArchiver = mock[BillHistoryArchiver[ConnectionIO]]

    when(billRepo.upsert(any[BillDO])).thenReturn(doobie.free.connection.pure(99L))
    when(cosponsorRepo.replaceAll(any[Long], any[List[BillCosponsorDO]])).thenReturn(doobie.free.connection.pure(()))
    when(subjectRepo.replaceAll(any[Long], any[List[BillSubjectDO]])).thenReturn(doobie.free.connection.pure(()))

    val cosponsors =
      List(BillCosponsorDO(billId = 0L, memberId = 10L, isOriginalCosponsor = Some(true), sponsorshipDate = None))
    val persister = makePersister(billRepo, cosponsorRepo, subjectRepo, historyArchiver)
    persister.persistBill(baseBillDO, List.empty, cosponsors, "118-HR-1", isNew = true).unsafeRunSync()

    verify(cosponsorRepo, times(1)).replaceAll(
      org.mockito.ArgumentMatchers.eq(99L),
      any[List[BillCosponsorDO]],
    )
  }

}
