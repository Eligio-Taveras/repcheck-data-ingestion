package repcheck.ingestion.bills.metadata.pipeline

import java.time.Instant
import java.util.UUID

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
import repcheck.ingestion.bills.metadata.api.BillsApiClient
import repcheck.ingestion.bills.metadata.config.BillMetadataConfig
import repcheck.ingestion.common.api.FetchParams
import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}
import repcheck.ingestion.common.placeholders.{EntityRepository, PlaceholderCreator}
import repcheck.members.common.persistence.MemberRepository
import repcheck.shared.models.congress.bill.TextVersionCode
import repcheck.shared.models.congress.common.{BillType, Chamber}
import repcheck.shared.models.congress.dos.bill.{BillCosponsorDO, BillDO, BillSubjectDO}
import repcheck.shared.models.congress.dos.member.MemberDO
import repcheck.shared.models.congress.dto.bill.{
  BillDetailDTO,
  BillListItemDTO,
  CoSponsorDTO,
  LegislativeSubjectDTO,
  SponsorDTO,
}
import repcheck.shared.models.congress.dto.common.PaginationInfoDTO
import repcheck.shared.models.placeholder.HasPlaceholder

class BillMetadataProcessorSpec extends AnyFlatSpec with Matchers with MockitoSugar {

  private val testXa: Transactor[IO] = Transactor.fromDriverManager[IO](
    driver = "org.h2.Driver",
    url = "jdbc:h2:mem:test;DB_CLOSE_DELAY=-1",
    user = "",
    password = "",
    logHandler = None,
  )

  private val runId         = 12345L
  private val correlationId = UUID.randomUUID()
  private val config        = BillMetadataConfig(lookbackDays = 30, parallelism = 1)

  /**
   * Stub PlaceholderCreator that records calls and always returns IO.unit. Avoids Mockito issues with Scala 3 using
   * clauses. Uses AtomicReference to avoid WartRemover mutable collection restrictions.
   */
  private class StubPlaceholderCreator extends PlaceholderCreator[IO] {
    private val callsRef = new java.util.concurrent.atomic.AtomicReference[List[String]](List.empty)

    def ensureExists[T <: Product](
      naturalKey: String,
      repository: EntityRepository[IO, T],
    )(using HasPlaceholder[T]): IO[Unit] = IO {
      val _ = callsRef.updateAndGet(keys => keys :+ naturalKey)
    }

    def callCount: Int                      = callsRef.get().size
    def calledWith: List[String]            = callsRef.get()
    def wasCalledWith(key: String): Boolean = callsRef.get().contains(key)
  }

  private def makeMemberDO(memberId: Long, bioguideId: String): MemberDO =
    MemberDO(
      memberId = memberId,
      naturalKey = bioguideId,
      firstName = None,
      lastName = None,
      directOrderName = None,
      invertedOrderName = None,
      honorificName = None,
      birthYear = None,
      currentParty = None,
      state = None,
      district = None,
      imageUrl = None,
      imageAttribution = None,
      officialUrl = None,
      updateDate = None,
      createdAt = None,
      updatedAt = None,
    )

  private case class TestFixture(
    apiClient: BillsApiClient[IO],
    billRepo: BillRepository[ConnectionIO],
    cosponsorRepo: BillCosponsorRepository[ConnectionIO],
    subjectRepo: BillSubjectRepository[ConnectionIO],
    historyArchiver: BillHistoryArchiver[ConnectionIO],
    memberRepo: MemberRepository,
    stubPlaceholderCreator: StubPlaceholderCreator,
    memberEntityRepo: EntityRepository[IO, MemberDO],
    logger: PipelineLogger[IO],
  ) {

    def processor: BillMetadataProcessor[IO] =
      new BillMetadataProcessor[IO](
        apiClient = apiClient,
        billRepo = billRepo,
        cosponsorRepo = cosponsorRepo,
        subjectRepo = subjectRepo,
        historyArchiver = historyArchiver,
        memberRepo = memberRepo,
        placeholderCreator = stubPlaceholderCreator,
        memberEntityRepo = memberEntityRepo,
        xa = testXa,
        config = config,
        logger = logger,
      )

  }

  private def createFixture(): TestFixture = {
    val loggerMock = mock[PipelineLogger[IO]]
    when(loggerMock.info(any[LogContext], anyString())).thenReturn(IO.unit)
    when(loggerMock.warn(any[LogContext], anyString())).thenReturn(IO.unit)
    when(loggerMock.error(any[LogContext], anyString(), any[Option[Throwable]])).thenReturn(IO.unit)
    when(loggerMock.debug(any[LogContext], anyString())).thenReturn(IO.unit)

    val billRepoMock = mock[BillRepository[ConnectionIO]]
    // Default stubs for BillPersister.applyExpectedVersionFloor — without these, the new floor-write
    // path called from inside persistBill returns null when bills with originChamber = Some(...) flow
    // through processOneBill, which the fixture's bills do. Tests that exercise the floor logic
    // explicitly override these stubs.
    when(billRepoMock.findExpectedVersion(anyString())).thenReturn(doobie.free.connection.pure(None))
    when(billRepoMock.updateExpectedVersion(anyString(), any[TextVersionCode]))
      .thenReturn(doobie.free.connection.unit)

    val apiClientMock = mock[BillsApiClient[IO]]
    // The processor always calls fetchSubjects; default to empty so tests not concerned with subjects still run.
    when(apiClientMock.fetchSubjects(anyString())).thenReturn(IO.pure(List.empty[LegislativeSubjectDTO]))

    TestFixture(
      apiClient = apiClientMock,
      billRepo = billRepoMock,
      cosponsorRepo = mock[BillCosponsorRepository[ConnectionIO]],
      subjectRepo = mock[BillSubjectRepository[ConnectionIO]],
      historyArchiver = mock[BillHistoryArchiver[ConnectionIO]],
      memberRepo = mock[MemberRepository],
      stubPlaceholderCreator = new StubPlaceholderCreator,
      memberEntityRepo = mock[EntityRepository[IO, MemberDO]],
      logger = loggerMock,
    )
  }

  private def makeListItem(
    congress: Int = 118,
    billType: String = "hr",
    number: String = "1",
    title: String = "Test Bill",
    updateDate: Option[String] = Some("2024-06-01T00:00:00Z"),
  ): BillListItemDTO =
    BillListItemDTO(
      congress = congress,
      number = number,
      billType = billType,
      originChamber = Some("House"),
      originChamberCode = Some("H"),
      title = title,
      latestAction = None,
      url = s"https://api.congress.gov/v3/bill/$congress/$billType/$number",
      updateDate = updateDate,
      updateDateIncludingText = None,
    )

  // Hydrated stored bill: every metadata-pipeline-owned field (sponsor_member_id, introduced_date,
  // latest_action_text, update_date) is populated so `BillPlaceholder.isPlaceholder` returns false
  // and the date-comparison branches in `evaluateAndProcess` are reachable. Tests that want to
  // exercise the placeholder branch construct rows from `HasPlaceholder[BillDO].placeholder(...)`
  // directly instead of using this helper.
  private def makeStoredBill(
    naturalKey: String = "118-HR-1",
    billId: Long = 42L,
    updateDate: Option[Instant],
  ): BillDO =
    BillDO(
      billId = billId,
      naturalKey = naturalKey,
      congress = 118,
      billType = BillType.HR,
      number = "1",
      title = "Old Title",
      originChamber = Some(Chamber.House),
      originChamberCode = Some("H"),
      introducedDate = Some(java.time.LocalDate.of(2024, 1, 1)),
      policyArea = None,
      latestActionDate = None,
      latestActionText = Some("Referred to committee"),
      constitutionalAuthorityText = None,
      sponsorMemberId = Some(100L),
      textVersionType = None,
      updateDate = updateDate,
      updateDateIncludingText = None,
      legislationUrl = None,
      apiUrl = None,
      createdAt = None,
      updatedAt = None,
      latestTextVersionId = None,
    )

  private def makeDetailDTO(
    congress: Int = 118,
    billType: String = "hr",
    number: String = "1",
    title: String = "Test Bill",
    updateDate: Option[String] = Some("2024-06-01T00:00:00Z"),
    cosponsorsUrl: Option[String] = None,
    sponsors: Option[List[SponsorDTO]] = None,
  ): BillDetailDTO =
    BillDetailDTO(
      congress = congress,
      number = number,
      billType = billType,
      latestAction = None,
      originChamber = Some("House"),
      originChamberCode = Some("H"),
      title = title,
      updateDate = updateDate,
      updateDateIncludingText = None,
      url = s"https://api.congress.gov/v3/bill/$congress/$billType/$number",
      introducedDate = None,
      policyArea = None,
      sponsors = sponsors,
      cosponsors = cosponsorsUrl.map(url => PaginationInfoDTO(count = Some(1), url = Some(url))),
      subjects = None,
      summaries = None,
      actions = None,
      committees = None,
      textVersions = None,
      titles = None,
      constitutionalAuthorityStatementText = None,
      cboCostEstimates = None,
      committeeReports = None,
      relatedBills = None,
      legislationUrl = None,
    )

  private def stubBasicRepos(f: TestFixture, storedBill: Option[BillDO] = None): Unit = {
    when(f.billRepo.findByBillId(anyString())).thenReturn(doobie.free.connection.pure(storedBill))
    when(f.billRepo.upsert(any[BillDO])).thenReturn(doobie.free.connection.pure(99L))
    when(f.cosponsorRepo.replaceAll(any[Long], any[List[BillCosponsorDO]])).thenReturn(doobie.free.connection.pure(()))
    when(f.subjectRepo.replaceAll(any[Long], any[List[BillSubjectDO]])).thenReturn(doobie.free.connection.pure(()))
    val _ = when(f.historyArchiver.archiveBill(anyString())).thenReturn(doobie.free.connection.pure(1L))
  }

  "processListItem" should "succeed for a new bill not in DB" in {
    val f        = createFixture()
    val listItem = makeListItem()
    val detail   = makeDetailDTO()
    stubBasicRepos(f)
    when(f.apiClient.fetchDetail(anyString())).thenReturn(IO.pure(detail))

    val result = f.processor.processListItem(listItem, correlationId).unsafeRunSync()
    result.isSucceeded shouldBe true
  }

  it should "succeed for an updated bill with newer updateDate" in {
    val f          = createFixture()
    val listItem   = makeListItem(updateDate = Some("2024-06-01T00:00:00Z"))
    val storedBill = makeStoredBill(updateDate = Some(Instant.parse("2024-01-01T00:00:00Z")))
    val detail     = makeDetailDTO()
    stubBasicRepos(f, storedBill = Some(storedBill))
    when(f.apiClient.fetchDetail(anyString())).thenReturn(IO.pure(detail))

    val result = f.processor.processListItem(listItem, correlationId).unsafeRunSync()
    result.isSucceeded shouldBe true
  }

  it should "skip unchanged bill with same updateDate" in {
    val f          = createFixture()
    val listItem   = makeListItem(updateDate = Some("2024-01-01T00:00:00Z"))
    val storedBill = makeStoredBill(updateDate = Some(Instant.parse("2024-01-01T00:00:00Z")))
    when(f.billRepo.findByBillId(anyString())).thenReturn(doobie.free.connection.pure(Some(storedBill)))

    val result = f.processor.processListItem(listItem, correlationId).unsafeRunSync()
    result.isSkipped shouldBe true
  }

  it should "skip bill when API updateDate is older than stored" in {
    val f          = createFixture()
    val listItem   = makeListItem(updateDate = Some("2023-06-01T00:00:00Z"))
    val storedBill = makeStoredBill(updateDate = Some(Instant.parse("2024-01-01T00:00:00Z")))
    when(f.billRepo.findByBillId(anyString())).thenReturn(doobie.free.connection.pure(Some(storedBill)))

    val result = f.processor.processListItem(listItem, correlationId).unsafeRunSync()
    result.isSkipped shouldBe true
  }

  it should "backfill a placeholder bill even if API updateDate is older than stored" in {
    // Placeholder rows from bill-text-availability-checker / bill-summary-pipeline carry a stored
    // update_date set to their insertion time, which is by definition newer than the API's real
    // updateDate for any older bill. BillPlaceholder.isPlaceholder is the canonical detector. This
    // test pins the behavior: a stored bill matching the placeholder shape MUST be routed through
    // the update path regardless of the date comparison so the detail fields get backfilled.
    val f        = createFixture()
    val listItem = makeListItem(updateDate = Some("2023-06-01T00:00:00Z"))
    // A canonical placeholder: every detail field empty/None, only natural-key + update_date set.
    val placeholderStored =
      repcheck.shared.models.placeholder
        .HasPlaceholder[BillDO]
        .placeholder("118-HR-1")
        .copy(
          billId = 42L,
          congress = 118,
          billType = repcheck.shared.models.congress.common.BillType.HR,
          number = "1",
          updateDate = Some(Instant.parse("2024-01-01T00:00:00Z")),
        )
    val detail = makeDetailDTO()

    stubBasicRepos(f, storedBill = Some(placeholderStored))
    when(f.apiClient.fetchDetail(anyString())).thenReturn(IO.pure(detail))

    val result = f.processor.processListItem(listItem, correlationId).unsafeRunSync()

    val _ = result.isSucceeded shouldBe true
    // Detail fetch happened (slow path), proving the placeholder branch was taken.
    val _ = verify(f.apiClient, times(1)).fetchDetail(anyString())
    verify(f.logger, times(1)).info(any[LogContext], org.mockito.ArgumentMatchers.contains("Placeholder bill detected"))
  }

  it should "not fetch detail for unchanged bills" in {
    val f          = createFixture()
    val listItem   = makeListItem(updateDate = Some("2024-01-01T00:00:00Z"))
    val storedBill = makeStoredBill(updateDate = Some(Instant.parse("2024-01-01T00:00:00Z")))
    when(f.billRepo.findByBillId(anyString())).thenReturn(doobie.free.connection.pure(Some(storedBill)))

    val _ = f.processor.processListItem(listItem, correlationId).unsafeRunSync()
    verify(f.apiClient, never()).fetchDetail(anyString())
  }

  it should "fetch detail for new bills" in {
    val f        = createFixture()
    val listItem = makeListItem()
    val detail   = makeDetailDTO()
    stubBasicRepos(f)
    when(f.apiClient.fetchDetail(anyString())).thenReturn(IO.pure(detail))

    val _ = f.processor.processListItem(listItem, correlationId).unsafeRunSync()
    verify(f.apiClient, times(1)).fetchDetail(anyString())
  }

  it should "return Failed when DTO-to-DO conversion fails" in {
    val f        = createFixture()
    val listItem = makeListItem(title = "")
    val detail   = makeDetailDTO(title = "")
    when(f.billRepo.findByBillId(anyString())).thenReturn(doobie.free.connection.pure(Option.empty[BillDO]))
    when(f.apiClient.fetchDetail(anyString())).thenReturn(IO.pure(detail))

    val result = f.processor.processListItem(listItem, correlationId).attempt.unsafeRunSync()
    result.isLeft shouldBe true
  }

  it should "archive history before upsert for updated bills" in {
    val f          = createFixture()
    val listItem   = makeListItem(updateDate = Some("2024-06-01T00:00:00Z"))
    val storedBill = makeStoredBill(updateDate = Some(Instant.parse("2024-01-01T00:00:00Z")))
    val detail     = makeDetailDTO()
    stubBasicRepos(f, storedBill = Some(storedBill))
    when(f.apiClient.fetchDetail(anyString())).thenReturn(IO.pure(detail))

    val _ = f.processor.processListItem(listItem, correlationId).unsafeRunSync()
    verify(f.historyArchiver, times(1)).archiveBill(anyString())
  }

  it should "not archive history for new bills" in {
    val f        = createFixture()
    val listItem = makeListItem()
    val detail   = makeDetailDTO()
    stubBasicRepos(f)
    when(f.apiClient.fetchDetail(anyString())).thenReturn(IO.pure(detail))

    val _ = f.processor.processListItem(listItem, correlationId).unsafeRunSync()
    verify(f.historyArchiver, never()).archiveBill(anyString())
  }

  it should "create sponsor placeholder for unknown member" in {
    val f        = createFixture()
    val listItem = makeListItem()
    val detail = makeDetailDTO(
      sponsors = Some(
        List(
          SponsorDTO.MemberSponsorDTO(
            bioguideId = "S001234",
            firstName = None,
            lastName = None,
            fullName = Some("Sen. Test"),
            middleName = None,
            isByRequest = None,
            party = None,
            state = None,
            district = None,
            url = None,
          )
        )
      )
    )
    stubBasicRepos(f)
    when(f.apiClient.fetchDetail(anyString())).thenReturn(IO.pure(detail))
    when(f.memberRepo.findByBioguideId(anyString()))
      .thenReturn(doobie.free.connection.pure(Some(makeMemberDO(100L, "S001234"))))

    val _ = f.processor.processListItem(listItem, correlationId).unsafeRunSync()
    val _ = f.stubPlaceholderCreator.callCount shouldBe 1
    f.stubPlaceholderCreator.wasCalledWith("S001234") shouldBe true
  }

  it should "warn and set sponsorMemberId to None when member lookup returns None for sponsor" in {
    val f        = createFixture()
    val listItem = makeListItem()
    val detail = makeDetailDTO(
      sponsors = Some(
        List(
          SponsorDTO.MemberSponsorDTO(
            bioguideId = "GHOST001",
            firstName = None,
            lastName = None,
            fullName = Some("Sen. Ghost"),
            middleName = None,
            isByRequest = None,
            party = None,
            state = None,
            district = None,
            url = None,
          )
        )
      )
    )
    stubBasicRepos(f)
    when(f.apiClient.fetchDetail(anyString())).thenReturn(IO.pure(detail))
    // Member lookup returns None — placeholder was created but ID not found
    when(f.memberRepo.findByBioguideId("GHOST001"))
      .thenReturn(doobie.free.connection.pure(Option.empty[MemberDO]))

    val result = f.processor.processListItem(listItem, correlationId).unsafeRunSync()
    val _      = result.isSucceeded shouldBe true
    verify(f.logger, times(1)).warn(any[LogContext], org.mockito.ArgumentMatchers.contains("GHOST001"))
  }

  it should "create cosponsor placeholders for unknown members" in {
    val cosponsorUrl = "https://api.congress.gov/v3/bill/118/hr/1/cosponsors"
    val f            = createFixture()
    val listItem     = makeListItem()
    val detail       = makeDetailDTO(cosponsorsUrl = Some(cosponsorUrl))
    val cosponsors = List(
      CoSponsorDTO("C000001", None, None, None, Some(true), None, None, Some("2024-01-15"), None, None),
      CoSponsorDTO("C000002", None, None, None, Some(false), None, None, Some("2024-02-01"), None, None),
    )
    stubBasicRepos(f)
    when(f.apiClient.fetchDetail(anyString())).thenReturn(IO.pure(detail))
    when(f.apiClient.fetchCosponsors(anyString())).thenReturn(IO.pure(cosponsors))
    when(f.memberRepo.findByBioguideId(anyString()))
      .thenReturn(doobie.free.connection.pure(Some(makeMemberDO(200L, "ANY"))))

    val _ = f.processor.processListItem(listItem, correlationId).unsafeRunSync()
    f.stubPlaceholderCreator.callCount shouldBe 2
  }

  it should "warn and filter out cosponsor when member lookup returns None" in {
    val cosponsorUrl = "https://api.congress.gov/v3/bill/118/hr/1/cosponsors"
    val f            = createFixture()
    val listItem     = makeListItem()
    val detail       = makeDetailDTO(cosponsorsUrl = Some(cosponsorUrl))
    val cosponsors = List(
      CoSponsorDTO("FOUND01", None, None, None, Some(true), None, None, Some("2024-01-15"), None, None),
      CoSponsorDTO("NOTFOUND", None, None, None, Some(false), None, None, Some("2024-02-01"), None, None),
    )
    stubBasicRepos(f)
    when(f.apiClient.fetchDetail(anyString())).thenReturn(IO.pure(detail))
    when(f.apiClient.fetchCosponsors(anyString())).thenReturn(IO.pure(cosponsors))
    // First cosponsor found, second returns None
    when(f.memberRepo.findByBioguideId("FOUND01"))
      .thenReturn(doobie.free.connection.pure(Some(makeMemberDO(200L, "FOUND01"))))
    when(f.memberRepo.findByBioguideId("NOTFOUND"))
      .thenReturn(doobie.free.connection.pure(Option.empty[MemberDO]))

    val result = f.processor.processListItem(listItem, correlationId).unsafeRunSync()
    val _      = result.isSucceeded shouldBe true
    // Should warn about the not-found cosponsor
    verify(f.logger, times(1)).warn(any[LogContext], org.mockito.ArgumentMatchers.contains("NOTFOUND"))
  }

  it should "handle single bill failure without halting the stream" in {
    val f        = createFixture()
    val goodItem = makeListItem(number = "1")
    val badItem  = makeListItem(number = "2")

    when(f.billRepo.findByBillId("118-HR-1")).thenReturn(doobie.free.connection.pure(Option.empty[BillDO]))
    when(f.billRepo.findByBillId("118-HR-2")).thenReturn(doobie.free.connection.pure(Option.empty[BillDO]))
    when(f.billRepo.upsert(any[BillDO])).thenReturn(doobie.free.connection.pure(99L))
    when(f.cosponsorRepo.replaceAll(any[Long], any[List[BillCosponsorDO]])).thenReturn(doobie.free.connection.pure(()))
    when(f.subjectRepo.replaceAll(any[Long], any[List[BillSubjectDO]])).thenReturn(doobie.free.connection.pure(()))

    val goodDetail = makeDetailDTO(number = "1")
    val badDetail  = makeDetailDTO(number = "2", title = "")

    when(f.apiClient.fetchDetail(org.mockito.ArgumentMatchers.contains("bill/118/hr/1")))
      .thenReturn(IO.pure(goodDetail))
    when(f.apiClient.fetchDetail(org.mockito.ArgumentMatchers.contains("bill/118/hr/2")))
      .thenReturn(IO.pure(badDetail))

    when(f.apiClient.fetchAll(any[FetchParams]))
      .thenReturn(fs2.Stream.emits(List(goodItem, badItem)))

    val results = f.processor.streamAll(runId).compile.toList.unsafeRunSync()

    val _ = results.size shouldBe 2
    val _ = results.count(_.isSucceeded) should be >= 1
    results.count(_.isFailed) should be >= 1
  }

  "buildNaturalKey" should "construct correct natural key" in {
    val f        = createFixture()
    val listItem = makeListItem(congress = 118, billType = "hr", number = "1234")
    f.processor.buildNaturalKey(listItem) shouldBe "118-HR-1234"
  }

  it should "uppercase bill type" in {
    val f        = createFixture()
    val listItem = makeListItem(billType = "s")
    f.processor.buildNaturalKey(listItem) shouldBe "118-S-1"
  }

  "streamAll" should "use lookback window from config" in {
    val f = createFixture()
    when(f.apiClient.fetchAll(any[FetchParams]))
      .thenReturn(fs2.Stream.empty)

    val _ = f.processor.streamAll(runId).compile.toList.unsafeRunSync()
    verify(f.apiClient, times(1)).fetchAll(any[FetchParams])
  }

  it should "produce results with correct entity IDs" in {
    val f        = createFixture()
    val listItem = makeListItem(number = "5")
    when(f.apiClient.fetchAll(any[FetchParams]))
      .thenReturn(fs2.Stream.emit(listItem))
    when(f.billRepo.findByBillId("118-HR-5")).thenReturn(doobie.free.connection.pure(Option.empty[BillDO]))

    val detail = makeDetailDTO(number = "5")
    when(f.apiClient.fetchDetail(anyString())).thenReturn(IO.pure(detail))
    when(f.billRepo.upsert(any[BillDO])).thenReturn(doobie.free.connection.pure(99L))
    when(f.cosponsorRepo.replaceAll(any[Long], any[List[BillCosponsorDO]])).thenReturn(doobie.free.connection.pure(()))
    when(f.subjectRepo.replaceAll(any[Long], any[List[BillSubjectDO]])).thenReturn(doobie.free.connection.pure(()))

    val results = f.processor.streamAll(runId).compile.toList.unsafeRunSync()

    val _ = results.size shouldBe 1
    results.headOption.map(_.entityId) shouldBe Some("118-HR-5")
  }

  it should "log error and re-raise when page fetch fails (fail-loud, do not swallow)" in {
    // Earlier behavior: handleErrorWith returned Stream.empty, so a paginated backfill
    // crash looked like a successful run with partial results (exit code 0). That masked
    // real failures. Now the stream re-raises so the IOApp run exits non-zero and the
    // operator sees the truncation.
    val f    = createFixture()
    val boom = new RuntimeException("network timeout")
    when(f.apiClient.fetchAll(any[FetchParams]))
      .thenReturn(fs2.Stream.raiseError[IO](boom))

    val attempt = f.processor.streamAll(runId).compile.toList.attempt.unsafeRunSync()
    val _       = attempt.isLeft shouldBe true
    val _       = attempt.left.toOption.map(_.getMessage) shouldBe Some("network timeout")
    verify(f.logger, times(1)).error(
      any[LogContext],
      org.mockito.ArgumentMatchers.contains("Page fetch failed"),
      any[Option[Throwable]],
    )
  }

  it should "return empty stream when no bills match lookback" in {
    val f = createFixture()
    when(f.apiClient.fetchAll(any[FetchParams]))
      .thenReturn(fs2.Stream.empty)

    val results = f.processor.streamAll(runId).compile.toList.unsafeRunSync()
    results shouldBe empty
  }

  private def processorWithConfig(f: TestFixture, cfg: BillMetadataConfig): BillMetadataProcessor[IO] =
    new BillMetadataProcessor[IO](
      apiClient = f.apiClient,
      billRepo = f.billRepo,
      cosponsorRepo = f.cosponsorRepo,
      subjectRepo = f.subjectRepo,
      historyArchiver = f.historyArchiver,
      memberRepo = f.memberRepo,
      placeholderCreator = f.stubPlaceholderCreator,
      memberEntityRepo = f.memberEntityRepo,
      xa = testXa,
      config = cfg,
      logger = f.logger,
    )

  it should "drop bills below minCongress before any detail fetch" in {
    val f = createFixture()
    val processor =
      processorWithConfig(f, BillMetadataConfig(lookbackDays = 30, parallelism = 1, minCongress = Some(102)))
    val oldBill    = makeListItem(congress = 27, billType = "hjres", number = "5")
    val recentBill = makeListItem(congress = 118, billType = "hr", number = "1")

    stubBasicRepos(f)
    when(f.apiClient.fetchAll(any[FetchParams])).thenReturn(fs2.Stream.emits(List(oldBill, recentBill)))
    when(f.apiClient.fetchDetail(anyString())).thenReturn(IO.pure(makeDetailDTO()))

    val results = processor.streamAll(runId).compile.toList.unsafeRunSync()

    val _ = results.size shouldBe 1
    val _ = results.headOption.map(_.entityId) shouldBe Some("118-HR-1")
    verify(f.apiClient, never()).fetchDetail(org.mockito.ArgumentMatchers.contains("bill/27/"))
  }

  it should "let bills through when their congress equals minCongress (boundary inclusive)" in {
    val f = createFixture()
    val processor =
      processorWithConfig(f, BillMetadataConfig(lookbackDays = 30, parallelism = 1, minCongress = Some(102)))
    val boundary = makeListItem(congress = 102, billType = "hr", number = "1", title = "Boundary")

    stubBasicRepos(f)
    when(f.apiClient.fetchAll(any[FetchParams])).thenReturn(fs2.Stream.emit(boundary))
    when(f.apiClient.fetchDetail(anyString())).thenReturn(IO.pure(makeDetailDTO(congress = 102, title = "Boundary")))

    val results = processor.streamAll(runId).compile.toList.unsafeRunSync()

    val _ = results.size shouldBe 1
    results.headOption.map(_.entityId) shouldBe Some("102-HR-1")
  }

  it should "not filter any bills when minCongress is None" in {
    val f         = createFixture()
    val processor = processorWithConfig(f, BillMetadataConfig(lookbackDays = 30, parallelism = 1, minCongress = None))
    val oldBill   = makeListItem(congress = 27, billType = "hjres", number = "5", title = "Old")

    stubBasicRepos(f)
    when(f.apiClient.fetchAll(any[FetchParams])).thenReturn(fs2.Stream.emit(oldBill))
    when(f.apiClient.fetchDetail(anyString()))
      .thenReturn(IO.pure(makeDetailDTO(congress = 27, billType = "hjres", number = "5", title = "Old")))

    val results = processor.streamAll(runId).compile.toList.unsafeRunSync()

    results.size shouldBe 1
  }

  it should "log a Sweep progress checkpoint at every 250 results" in {
    val f     = createFixture()
    val items = (1 to 250).map(i => makeListItem(number = i.toString, updateDate = Some("2024-06-01T00:00:00Z"))).toList
    val storedBill = makeStoredBill(updateDate = Some(Instant.parse("2024-06-01T00:00:00Z")))

    when(f.apiClient.fetchAll(any[FetchParams])).thenReturn(fs2.Stream.emits(items))
    when(f.billRepo.findByBillId(anyString())).thenReturn(doobie.free.connection.pure(Some(storedBill)))

    val results = f.processor.streamAll(runId).compile.toList.unsafeRunSync()

    val _ = results.size shouldBe 250
    verify(f.logger, times(1)).info(any[LogContext], org.mockito.ArgumentMatchers.contains("Sweep progress: 250"))
  }

  it should "emit a debug log when a bill is filtered by minCongress" in {
    val f = createFixture()
    val processor =
      processorWithConfig(f, BillMetadataConfig(lookbackDays = 30, parallelism = 1, minCongress = Some(102)))
    val oldBill = makeListItem(congress = 27, billType = "hjres", number = "5")

    when(f.apiClient.fetchAll(any[FetchParams])).thenReturn(fs2.Stream.emit(oldBill))

    val _ = processor.streamAll(runId).compile.toList.unsafeRunSync()

    verify(f.logger, times(1))
      .debug(any[LogContext], org.mockito.ArgumentMatchers.contains("Filtered by minCongress=102"))
  }

  it should "fetch legislative subjects from the sub-endpoint and persist them" in {
    val f        = createFixture()
    val listItem = makeListItem()
    val detail   = makeDetailDTO()
    stubBasicRepos(f)
    when(f.apiClient.fetchDetail(anyString())).thenReturn(IO.pure(detail))
    when(f.apiClient.fetchSubjects(anyString()))
      .thenReturn(IO.pure(List(LegislativeSubjectDTO("Health care", Some("2024-01-01T00:00:00Z")))))

    val _ = f.processor.processListItem(listItem, correlationId).unsafeRunSync()

    val captor = org.mockito.ArgumentCaptor.forClass(classOf[List[BillSubjectDO]])
    val _      = verify(f.subjectRepo).replaceAll(any[Long], captor.capture())
    captor.getValue.map(_.subjectName) should contain("Health care")
  }

}
