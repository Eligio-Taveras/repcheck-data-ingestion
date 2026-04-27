package repcheck.ingestion.bills.metadata.pipeline

import cats.effect.IO
import cats.effect.unsafe.implicits.global

import doobie._

import org.mockito.ArgumentMatchers.{any, anyString, eq => eqTo}
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
import repcheck.shared.models.congress.bill.TextVersionCode
import repcheck.shared.models.congress.common.{BillType, Chamber}
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

  // ---------------------------------------------------------------------------
  // applyExpectedVersionFloor — derives the introduced-stage floor (House → IH,
  // Senate → IS) and writes when current expected is None or behind. Cooperates
  // with bill-summary-pipeline's advancing writes via TextVersionCode.progressionOrder.
  // ---------------------------------------------------------------------------

  "floorFor" should "map House to IH, Senate to IS" in {
    val billRepo        = mock[BillRepository[ConnectionIO]]
    val cosponsorRepo   = mock[BillCosponsorRepository[ConnectionIO]]
    val subjectRepo     = mock[BillSubjectRepository[ConnectionIO]]
    val historyArchiver = mock[BillHistoryArchiver[ConnectionIO]]
    val persister       = makePersister(billRepo, cosponsorRepo, subjectRepo, historyArchiver)

    val _ = persister.floorFor(Some(Chamber.House)) shouldBe Some(TextVersionCode.IH)
    persister.floorFor(Some(Chamber.Senate)) shouldBe Some(TextVersionCode.IS)
  }

  it should "return None for Joint, missing chamber, or any other unknown value" in {
    val billRepo        = mock[BillRepository[ConnectionIO]]
    val cosponsorRepo   = mock[BillCosponsorRepository[ConnectionIO]]
    val subjectRepo     = mock[BillSubjectRepository[ConnectionIO]]
    val historyArchiver = mock[BillHistoryArchiver[ConnectionIO]]
    val persister       = makePersister(billRepo, cosponsorRepo, subjectRepo, historyArchiver)

    val _ = persister.floorFor(Some(Chamber.Joint)) shouldBe None
    persister.floorFor(None) shouldBe None
  }

  "applyExpectedVersionFloor" should "skip when chamber is None (no floor to apply)" in {
    val billRepo        = mock[BillRepository[ConnectionIO]]
    val cosponsorRepo   = mock[BillCosponsorRepository[ConnectionIO]]
    val subjectRepo     = mock[BillSubjectRepository[ConnectionIO]]
    val historyArchiver = mock[BillHistoryArchiver[ConnectionIO]]

    when(billRepo.upsert(any[BillDO])).thenReturn(doobie.free.connection.pure(99L))
    when(cosponsorRepo.replaceAll(any[Long], any[List[BillCosponsorDO]])).thenReturn(doobie.free.connection.pure(()))
    when(subjectRepo.replaceAll(any[Long], any[List[BillSubjectDO]])).thenReturn(doobie.free.connection.pure(()))

    val persister = makePersister(billRepo, cosponsorRepo, subjectRepo, historyArchiver)
    // baseBillDO has originChamber = None → floor is None → no findExpectedVersion / updateExpectedVersion.
    persister.persistBill(baseBillDO, List.empty, List.empty, "118-HR-1", isNew = true).unsafeRunSync()

    val _ = verify(billRepo, never()).findExpectedVersion(anyString())
    verify(billRepo, never()).updateExpectedVersion(anyString(), any[TextVersionCode])
  }

  it should "write IH when origin is House and existing expected is None" in {
    val billRepo        = mock[BillRepository[ConnectionIO]]
    val cosponsorRepo   = mock[BillCosponsorRepository[ConnectionIO]]
    val subjectRepo     = mock[BillSubjectRepository[ConnectionIO]]
    val historyArchiver = mock[BillHistoryArchiver[ConnectionIO]]

    when(billRepo.upsert(any[BillDO])).thenReturn(doobie.free.connection.pure(99L))
    when(cosponsorRepo.replaceAll(any[Long], any[List[BillCosponsorDO]])).thenReturn(doobie.free.connection.pure(()))
    when(subjectRepo.replaceAll(any[Long], any[List[BillSubjectDO]])).thenReturn(doobie.free.connection.pure(()))
    when(billRepo.findExpectedVersion(anyString())).thenReturn(doobie.free.connection.pure(None))
    when(billRepo.updateExpectedVersion(anyString(), any[TextVersionCode])).thenReturn(doobie.free.connection.pure(()))

    val persister = makePersister(billRepo, cosponsorRepo, subjectRepo, historyArchiver)
    val houseBill = baseBillDO.copy(originChamber = Some(Chamber.House))
    persister.persistBill(houseBill, List.empty, List.empty, "118-HR-1", isNew = true).unsafeRunSync()

    verify(billRepo, times(1)).updateExpectedVersion(eqTo("118-HR-1"), eqTo(TextVersionCode.IH))
  }

  it should "write IS when origin is Senate and existing expected is None" in {
    val billRepo        = mock[BillRepository[ConnectionIO]]
    val cosponsorRepo   = mock[BillCosponsorRepository[ConnectionIO]]
    val subjectRepo     = mock[BillSubjectRepository[ConnectionIO]]
    val historyArchiver = mock[BillHistoryArchiver[ConnectionIO]]

    when(billRepo.upsert(any[BillDO])).thenReturn(doobie.free.connection.pure(99L))
    when(cosponsorRepo.replaceAll(any[Long], any[List[BillCosponsorDO]])).thenReturn(doobie.free.connection.pure(()))
    when(subjectRepo.replaceAll(any[Long], any[List[BillSubjectDO]])).thenReturn(doobie.free.connection.pure(()))
    when(billRepo.findExpectedVersion(anyString())).thenReturn(doobie.free.connection.pure(None))
    when(billRepo.updateExpectedVersion(anyString(), any[TextVersionCode])).thenReturn(doobie.free.connection.pure(()))

    val persister = makePersister(billRepo, cosponsorRepo, subjectRepo, historyArchiver)
    val senateBill =
      baseBillDO.copy(billType = BillType.S, originChamber = Some(Chamber.Senate), naturalKey = "118-S-1")
    persister.persistBill(senateBill, List.empty, List.empty, "118-S-1", isNew = true).unsafeRunSync()

    verify(billRepo, times(1)).updateExpectedVersion(eqTo("118-S-1"), eqTo(TextVersionCode.IS))
  }

  it should "skip the write when existing expected stage is at the floor (idempotent)" in {
    val billRepo        = mock[BillRepository[ConnectionIO]]
    val cosponsorRepo   = mock[BillCosponsorRepository[ConnectionIO]]
    val subjectRepo     = mock[BillSubjectRepository[ConnectionIO]]
    val historyArchiver = mock[BillHistoryArchiver[ConnectionIO]]

    when(billRepo.upsert(any[BillDO])).thenReturn(doobie.free.connection.pure(99L))
    when(cosponsorRepo.replaceAll(any[Long], any[List[BillCosponsorDO]])).thenReturn(doobie.free.connection.pure(()))
    when(subjectRepo.replaceAll(any[Long], any[List[BillSubjectDO]])).thenReturn(doobie.free.connection.pure(()))
    when(billRepo.findExpectedVersion(anyString())).thenReturn(doobie.free.connection.pure(Some(TextVersionCode.IH)))
    when(billRepo.updateExpectedVersion(anyString(), any[TextVersionCode])).thenReturn(doobie.free.connection.pure(()))

    val persister = makePersister(billRepo, cosponsorRepo, subjectRepo, historyArchiver)
    val houseBill = baseBillDO.copy(originChamber = Some(Chamber.House))
    persister.persistBill(houseBill, List.empty, List.empty, "118-HR-1", isNew = true).unsafeRunSync()

    // Existing IH == floor IH → progressionOrder comparison says don't downgrade or rewrite.
    verify(billRepo, never()).updateExpectedVersion(anyString(), any[TextVersionCode])
  }

  it should "skip the write when existing expected stage is more advanced than the floor (regression guard)" in {
    val billRepo        = mock[BillRepository[ConnectionIO]]
    val cosponsorRepo   = mock[BillCosponsorRepository[ConnectionIO]]
    val subjectRepo     = mock[BillSubjectRepository[ConnectionIO]]
    val historyArchiver = mock[BillHistoryArchiver[ConnectionIO]]

    when(billRepo.upsert(any[BillDO])).thenReturn(doobie.free.connection.pure(99L))
    when(cosponsorRepo.replaceAll(any[Long], any[List[BillCosponsorDO]])).thenReturn(doobie.free.connection.pure(()))
    when(subjectRepo.replaceAll(any[Long], any[List[BillSubjectDO]])).thenReturn(doobie.free.connection.pure(()))
    when(billRepo.findExpectedVersion(anyString())).thenReturn(doobie.free.connection.pure(Some(TextVersionCode.PL)))
    when(billRepo.updateExpectedVersion(anyString(), any[TextVersionCode])).thenReturn(doobie.free.connection.pure(()))

    val persister = makePersister(billRepo, cosponsorRepo, subjectRepo, historyArchiver)
    val houseBill = baseBillDO.copy(originChamber = Some(Chamber.House))
    persister.persistBill(houseBill, List.empty, List.empty, "118-HR-1", isNew = true).unsafeRunSync()

    // Existing PL > floor IH → never write IH back; protects against summary-pipeline → metadata-pipeline regression.
    verify(billRepo, never()).updateExpectedVersion(anyString(), any[TextVersionCode])
  }

  it should "perform the floor write inside the same transaction as the upsert" in {
    // The floor write happens via the same `ConnectionIO` flatMap chain in persistBill, so a failure in
    // the existing upsert would prevent the floor write from running. We exercise that here by stubbing
    // upsert to succeed but findExpectedVersion to raise — the test verifies the error propagates rather
    // than being silently caught. (The underlying TransactionRunner takes care of rollback.)
    val billRepo        = mock[BillRepository[ConnectionIO]]
    val cosponsorRepo   = mock[BillCosponsorRepository[ConnectionIO]]
    val subjectRepo     = mock[BillSubjectRepository[ConnectionIO]]
    val historyArchiver = mock[BillHistoryArchiver[ConnectionIO]]

    val findFailure = new repcheck.ingestion.bills.common.errors.InvalidBillNaturalKey("bad", "synthetic")
    when(billRepo.upsert(any[BillDO])).thenReturn(doobie.free.connection.pure(99L))
    when(cosponsorRepo.replaceAll(any[Long], any[List[BillCosponsorDO]])).thenReturn(doobie.free.connection.pure(()))
    when(subjectRepo.replaceAll(any[Long], any[List[BillSubjectDO]])).thenReturn(doobie.free.connection.pure(()))
    when(billRepo.findExpectedVersion(anyString()))
      .thenReturn(doobie.free.connection.raiseError[Option[TextVersionCode]](findFailure))

    val persister = makePersister(billRepo, cosponsorRepo, subjectRepo, historyArchiver)
    val houseBill = baseBillDO.copy(originChamber = Some(Chamber.House))

    val thrown = intercept[repcheck.ingestion.bills.common.errors.InvalidBillNaturalKey] {
      persister.persistBill(houseBill, List.empty, List.empty, "118-HR-1", isNew = true).unsafeRunSync()
    }
    thrown shouldBe findFailure
  }

}
