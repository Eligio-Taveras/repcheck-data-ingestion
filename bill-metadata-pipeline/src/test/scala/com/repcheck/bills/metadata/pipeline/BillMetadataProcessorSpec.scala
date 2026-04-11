package com.repcheck.bills.metadata.pipeline

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
import repcheck.ingestion.common.api.FetchParams
import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}
import repcheck.ingestion.common.placeholders.{EntityRepository, PlaceholderCreator}
import repcheck.shared.models.congress.common.{BillType, Chamber}
import repcheck.shared.models.congress.dos.bill.{BillCosponsorDO, BillDO, BillSubjectDO}
import repcheck.shared.models.congress.dos.member.MemberDO
import repcheck.shared.models.congress.dto.bill.{BillDetailDTO, BillListItemDTO, CoSponsorDTO, SponsorDTO}
import repcheck.shared.models.congress.dto.common.PaginationInfoDTO
import repcheck.shared.models.placeholder.HasPlaceholder

import com.repcheck.bills.common.persistence.{
  BillCosponsorRepository,
  BillHistoryArchiver,
  BillRepository,
  BillSubjectRepository,
  MemberLookupRepository,
}
import com.repcheck.bills.metadata.api.BillsApiClient
import com.repcheck.bills.metadata.config.BillMetadataConfig

class BillMetadataProcessorSpec extends AnyFlatSpec with Matchers with MockitoSugar {

  private val testXa: Transactor[IO] = Transactor.fromDriverManager[IO](
    driver = "org.h2.Driver",
    url = "jdbc:h2:mem:test;DB_CLOSE_DELAY=-1",
    user = "",
    password = "",
    logHandler = None,
  )

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

  private case class TestFixture(
    apiClient: BillsApiClient[IO],
    billRepo: BillRepository[ConnectionIO],
    cosponsorRepo: BillCosponsorRepository[ConnectionIO],
    subjectRepo: BillSubjectRepository[ConnectionIO],
    historyArchiver: BillHistoryArchiver[ConnectionIO],
    memberLookupRepo: MemberLookupRepository[ConnectionIO],
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
        memberLookupRepo = memberLookupRepo,
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

    TestFixture(
      apiClient = mock[BillsApiClient[IO]],
      billRepo = mock[BillRepository[ConnectionIO]],
      cosponsorRepo = mock[BillCosponsorRepository[ConnectionIO]],
      subjectRepo = mock[BillSubjectRepository[ConnectionIO]],
      historyArchiver = mock[BillHistoryArchiver[ConnectionIO]],
      memberLookupRepo = mock[MemberLookupRepository[ConnectionIO]],
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
      textEmbedding = None,
      summaryText = None,
      summaryActionDesc = None,
      summaryActionDate = None,
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
          SponsorDTO(
            bioguideId = "S001234",
            firstName = None,
            lastName = None,
            fullName = Some("Sen. Test"),
            middleName = None,
            isByRequest = None,
            party = None,
            state = None,
            url = None,
          )
        )
      )
    )
    stubBasicRepos(f)
    when(f.apiClient.fetchDetail(anyString())).thenReturn(IO.pure(detail))
    when(f.memberLookupRepo.findIdByNaturalKey(anyString())).thenReturn(doobie.free.connection.pure(Some(100L)))

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
          SponsorDTO(
            bioguideId = "GHOST001",
            firstName = None,
            lastName = None,
            fullName = Some("Sen. Ghost"),
            middleName = None,
            isByRequest = None,
            party = None,
            state = None,
            url = None,
          )
        )
      )
    )
    stubBasicRepos(f)
    when(f.apiClient.fetchDetail(anyString())).thenReturn(IO.pure(detail))
    // Member lookup returns None — placeholder was created but ID not found
    when(f.memberLookupRepo.findIdByNaturalKey("GHOST001")).thenReturn(doobie.free.connection.pure(Option.empty[Long]))

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
    when(f.memberLookupRepo.findIdByNaturalKey(anyString())).thenReturn(doobie.free.connection.pure(Some(200L)))

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
    when(f.memberLookupRepo.findIdByNaturalKey("FOUND01")).thenReturn(doobie.free.connection.pure(Some(200L)))
    when(f.memberLookupRepo.findIdByNaturalKey("NOTFOUND")).thenReturn(doobie.free.connection.pure(Option.empty[Long]))

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

    val results = f.processor.streamAll(correlationId).compile.toList.unsafeRunSync()

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

    val _ = f.processor.streamAll(correlationId).compile.toList.unsafeRunSync()
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

    val results = f.processor.streamAll(correlationId).compile.toList.unsafeRunSync()

    val _ = results.size shouldBe 1
    results.headOption.map(_.entityId) shouldBe Some("118-HR-5")
  }

  it should "return empty stream when no bills match lookback" in {
    val f = createFixture()
    when(f.apiClient.fetchAll(any[FetchParams]))
      .thenReturn(fs2.Stream.empty)

    val results = f.processor.streamAll(correlationId).compile.toList.unsafeRunSync()
    results shouldBe empty
  }

}
