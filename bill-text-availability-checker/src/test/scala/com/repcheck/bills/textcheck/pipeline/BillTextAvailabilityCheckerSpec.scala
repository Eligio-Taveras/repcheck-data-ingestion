package com.repcheck.bills.textcheck.pipeline

import java.util.UUID

import cats.effect.IO
import cats.effect.unsafe.implicits.global

import doobie._

import org.mockito.ArgumentMatchers.{any, anyInt, anyString}
import org.mockito.Mockito.{never, times, verify, when}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar

import repcheck.ingestion.common.events.IngestionEventPublisher
import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}
import repcheck.pipeline.models.events.BillTextAvailableEvent
import repcheck.shared.models.congress.bill.TextVersionCode
import repcheck.shared.models.congress.common.{BillType, Chamber}
import repcheck.shared.models.congress.dos.bill.BillDO
import repcheck.shared.models.congress.dto.bill.{FormatDTO, TextVersionDTO}

import com.repcheck.bills.common.persistence.BillRepository
import com.repcheck.bills.textcheck.api.BillTextApiClient
import com.repcheck.bills.textcheck.config.BillTextCheckerConfig

class BillTextAvailabilityCheckerSpec extends AnyFlatSpec with Matchers with MockitoSugar {

  private val testXa: Transactor[IO] = Transactor.fromDriverManager[IO](
    driver = "org.h2.Driver",
    url = "jdbc:h2:mem:textcheck;DB_CLOSE_DELAY=-1",
    user = "",
    password = "",
    logHandler = None,
  )

  private val correlationId = UUID.randomUUID()
  private val config        = BillTextCheckerConfig(parallelism = 1)

  private case class TestFixture(
    textApiClient: BillTextApiClient[IO],
    billRepo: BillRepository[ConnectionIO],
    eventPublisher: IngestionEventPublisher[IO],
    logger: PipelineLogger[IO],
  ) {

    def checker: BillTextAvailabilityChecker[IO] =
      new BillTextAvailabilityChecker[IO](
        textApiClient = textApiClient,
        billRepo = billRepo,
        eventPublisher = eventPublisher,
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
      textApiClient = mock[BillTextApiClient[IO]],
      billRepo = mock[BillRepository[ConnectionIO]],
      eventPublisher = mock[IngestionEventPublisher[IO]],
      logger = loggerMock,
    )
  }

  private def makeBill(
    naturalKey: String = "118-HR-1",
    billId: Long = 42L,
    congress: Int = 118,
    billType: BillType = BillType.HR,
    number: String = "1",
    textUrl: Option[String] = None,
    textVersionType: Option[TextVersionCode] = None,
  ): BillDO =
    BillDO(
      billId = billId,
      naturalKey = naturalKey,
      congress = congress,
      billType = billType,
      number = number,
      title = "Test Bill",
      originChamber = Some(Chamber.House),
      originChamberCode = Some("H"),
      introducedDate = None,
      policyArea = None,
      latestActionDate = None,
      latestActionText = None,
      constitutionalAuthorityText = None,
      sponsorMemberId = None,
      textUrl = textUrl,
      textFormat = None,
      textVersionType = textVersionType,
      textDate = None,
      textContent = None,
      textEmbedding = None,
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

  private def makeTextVersion(
    date: Option[String] = Some("2024-01-15T00:00:00Z"),
    formatType: String = "Formatted Text",
    url: String = "https://api.congress.gov/text",
    versionType: Option[String] = Some("IH"),
  ): TextVersionDTO =
    TextVersionDTO(
      date = date,
      formats = Some(List(FormatDTO(formatType, url))),
      type_ = versionType,
    )

  "checkBill" should "emit event and return Succeeded for bill with no prior text" in {
    val f        = createFixture()
    val bill     = makeBill(textVersionType = None, textUrl = None)
    val versions = List(makeTextVersion())

    when(f.textApiClient.fetchTextVersions(anyInt(), anyString(), anyString()))
      .thenReturn(IO.pure(versions))
    when(f.eventPublisher.billTextAvailable(any[BillTextAvailableEvent], any[UUID]))
      .thenReturn(IO.pure("msg-id-1"))

    val result = f.checker.checkBill(bill, correlationId).unsafeRunSync()
    val _      = result.isSucceeded shouldBe true
    val _      = result.entityId shouldBe "118-HR-1"
    verify(f.eventPublisher, times(1)).billTextAvailable(any[BillTextAvailableEvent], any[UUID])
  }

  it should "emit event with previousVersionCode when version changes" in {
    val f = createFixture()
    val bill = makeBill(
      textVersionType = Some(TextVersionCode.IH),
      textUrl = Some("https://old-url.com"),
    )
    val versions = List(makeTextVersion(versionType = Some("EH"), url = "https://new-url.com"))

    when(f.textApiClient.fetchTextVersions(anyInt(), anyString(), anyString()))
      .thenReturn(IO.pure(versions))
    when(f.eventPublisher.billTextAvailable(any[BillTextAvailableEvent], any[UUID]))
      .thenReturn(IO.pure("msg-id-2"))

    val result = f.checker.checkBill(bill, correlationId).unsafeRunSync()
    val _      = result.isSucceeded shouldBe true

    val captor       = org.mockito.ArgumentCaptor.forClass(classOf[BillTextAvailableEvent])
    val _            = verify(f.eventPublisher).billTextAvailable(captor.capture(), any[UUID])
    val emittedEvent = captor.getValue
    val _            = emittedEvent.previousVersionCode shouldBe Some("IH")
    emittedEvent.versionCode shouldBe "EH"
  }

  it should "return Skipped when text version is unchanged" in {
    val f = createFixture()
    val bill = makeBill(
      textVersionType = Some(TextVersionCode.IH),
      textUrl = Some("https://example.com/text"),
    )
    val versions = List(makeTextVersion(versionType = Some("IH")))

    when(f.textApiClient.fetchTextVersions(anyInt(), anyString(), anyString()))
      .thenReturn(IO.pure(versions))

    val result = f.checker.checkBill(bill, correlationId).unsafeRunSync()
    val _      = result.isSkipped shouldBe true
    verify(f.eventPublisher, never()).billTextAvailable(any[BillTextAvailableEvent], any[UUID])
  }

  it should "return Skipped when API returns empty text versions" in {
    val f    = createFixture()
    val bill = makeBill()

    when(f.textApiClient.fetchTextVersions(anyInt(), anyString(), anyString()))
      .thenReturn(IO.pure(List.empty))

    val result = f.checker.checkBill(bill, correlationId).unsafeRunSync()
    result.isSkipped shouldBe true
  }

  it should "return Failed when API call throws an error" in {
    val f    = createFixture()
    val bill = makeBill()

    when(f.textApiClient.fetchTextVersions(anyInt(), anyString(), anyString()))
      .thenReturn(IO.raiseError(new RuntimeException("API unavailable")))

    val result = f.checker.checkBill(bill, correlationId).unsafeRunSync()
    val _      = result.isFailed shouldBe true
    result.entityId shouldBe "118-HR-1"
  }

  it should "propagate event publish error as Failed" in {
    val f        = createFixture()
    val bill     = makeBill(textVersionType = None)
    val versions = List(makeTextVersion())

    when(f.textApiClient.fetchTextVersions(anyInt(), anyString(), anyString()))
      .thenReturn(IO.pure(versions))
    when(f.eventPublisher.billTextAvailable(any[BillTextAvailableEvent], any[UUID]))
      .thenReturn(IO.raiseError(new RuntimeException("Pub/Sub down")))

    val result = f.checker.checkBill(bill, correlationId).unsafeRunSync()
    result.isFailed shouldBe true
  }

  it should "include correct event fields" in {
    val f = createFixture()
    val bill = makeBill(
      naturalKey = "118-HR-5",
      congress = 118,
      number = "5",
      textVersionType = None,
    )
    val versions = List(
      makeTextVersion(
        versionType = Some("IH"),
        url = "https://api.congress.gov/text/118/hr/5",
        formatType = "Formatted Text",
      )
    )

    when(f.textApiClient.fetchTextVersions(anyInt(), anyString(), anyString()))
      .thenReturn(IO.pure(versions))
    when(f.eventPublisher.billTextAvailable(any[BillTextAvailableEvent], any[UUID]))
      .thenReturn(IO.pure("msg-id"))

    val _ = f.checker.checkBill(bill, correlationId).unsafeRunSync()

    val captor = org.mockito.ArgumentCaptor.forClass(classOf[BillTextAvailableEvent])
    val _      = verify(f.eventPublisher).billTextAvailable(captor.capture(), any[UUID])
    val event  = captor.getValue
    val _      = event.billId shouldBe "118-HR-5"
    val _      = event.congress shouldBe 118
    val _      = event.textUrl shouldBe "https://api.congress.gov/text/118/hr/5"
    val _      = event.textFormat shouldBe "Formatted Text"
    val _      = event.versionCode shouldBe "IH"
    event.previousVersionCode shouldBe None
  }

  it should "pass billType apiValue to the text API client" in {
    val f    = createFixture()
    val bill = makeBill(billType = BillType.S, number = "100", naturalKey = "118-S-100")

    when(f.textApiClient.fetchTextVersions(anyInt(), anyString(), anyString()))
      .thenReturn(IO.pure(List.empty))

    val _ = f.checker.checkBill(bill, correlationId).unsafeRunSync()
    verify(f.textApiClient).fetchTextVersions(118, "s", "100")
  }

  "checkAll" should "stream results for all bills" in {
    val f = createFixture()
    val bills = List(
      makeBill(naturalKey = "118-HR-1", number = "1"),
      makeBill(naturalKey = "118-HR-2", number = "2"),
    )

    when(f.billRepo.findBillsNeedingTextCheck())
      .thenReturn(doobie.free.connection.pure(bills))
    when(f.textApiClient.fetchTextVersions(anyInt(), anyString(), anyString()))
      .thenReturn(IO.pure(List.empty))

    val results = f.checker.checkAll(correlationId).compile.toList.unsafeRunSync()
    val _       = results.size shouldBe 2
    results.foreach(_.isSkipped shouldBe true)
  }

  it should "not halt stream when one bill fails" in {
    val f = createFixture()
    val bills = List(
      makeBill(naturalKey = "118-HR-1", number = "1"),
      makeBill(naturalKey = "118-HR-2", number = "2"),
    )

    when(f.billRepo.findBillsNeedingTextCheck())
      .thenReturn(doobie.free.connection.pure(bills))

    // First bill fails, second succeeds with empty versions
    when(f.textApiClient.fetchTextVersions(118, "hr", "1"))
      .thenReturn(IO.raiseError(new RuntimeException("API error")))
    when(f.textApiClient.fetchTextVersions(118, "hr", "2"))
      .thenReturn(IO.pure(List.empty))

    val results = f.checker.checkAll(correlationId).compile.toList.unsafeRunSync()
    val _       = results.size shouldBe 2
    val _       = results.count(_.isFailed) shouldBe 1
    results.count(_.isSkipped) shouldBe 1
  }

  it should "complete with empty stream when no bills need checking" in {
    val f = createFixture()

    when(f.billRepo.findBillsNeedingTextCheck())
      .thenReturn(doobie.free.connection.pure(List.empty[BillDO]))

    val results = f.checker.checkAll(correlationId).compile.toList.unsafeRunSync()
    results shouldBe empty
  }

}
