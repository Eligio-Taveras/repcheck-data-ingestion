package com.repcheck.bills.text.pipeline

import java.util.UUID

import cats.effect.IO
import cats.effect.unsafe.implicits.global

import doobie._

import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.{any, anyString}
import org.mockito.Mockito.{never, times, verify, when}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar
import repcheck.ingestion.common.events.IngestionEventPublisher
import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}
import repcheck.pipeline.models.events.{BillTextAvailableEvent, BillTextIngestedEvent}
import repcheck.pipeline.models.metadata.ProcessingResult
import repcheck.shared.models.congress.dos.bill.BillTextVersionDO

import com.repcheck.bills.common.persistence.BillTextVersionRepository
import com.repcheck.bills.text.download.BillTextDownloader
import com.repcheck.bills.text.errors.{BillTextProcessingFailed, TextDownloadFailed}

class BillTextProcessorSpec extends AnyFlatSpec with Matchers with MockitoSugar {

  private val testXa: Transactor[IO] = Transactor.fromDriverManager[IO](
    driver = "org.h2.Driver",
    url = "jdbc:h2:mem:test-text-processor;DB_CLOSE_DELAY=-1",
    user = "",
    password = "",
    logHandler = None,
  )

  private val correlationId = UUID.randomUUID()

  private case class TestFixture(
    downloader: BillTextDownloader[IO],
    repository: BillTextVersionRepository[ConnectionIO],
    eventPublisher: IngestionEventPublisher[IO],
    logger: PipelineLogger[IO],
  ) {

    def processor: BillTextProcessor[IO] =
      new BillTextProcessor[IO](
        downloader = downloader,
        repository = repository,
        eventPublisher = eventPublisher,
        xa = testXa,
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
      downloader = mock[BillTextDownloader[IO]],
      repository = mock[BillTextVersionRepository[ConnectionIO]],
      eventPublisher = mock[IngestionEventPublisher[IO]],
      logger = loggerMock,
    )
  }

  private def makeEvent(
    billId: String = "118-HR-1",
    congress: Int = 118,
    textUrl: String = "https://api.congress.gov/v3/bill/118/hr/1/text/ih",
    textFormat: String = "Formatted Text",
    versionCode: String = "ih",
    previousVersionCode: Option[String] = None,
  ): BillTextAvailableEvent =
    BillTextAvailableEvent(
      billId = billId,
      congress = congress,
      textUrl = textUrl,
      textFormat = textFormat,
      versionCode = versionCode,
      previousVersionCode = previousVersionCode,
    )

  private def stubSuccessfulFlow(f: TestFixture, content: String = "Bill text content here"): Unit = {
    when(f.downloader.download(anyString(), anyString(), any[UUID])).thenReturn(IO.pure(content))
    when(f.repository.insertVersion(any[BillTextVersionDO])).thenReturn(doobie.free.connection.pure(1L))
    val _ = when(f.eventPublisher.billTextIngested(any[BillTextIngestedEvent], any[UUID]))
      .thenReturn(IO.pure("msg-id-123"))
  }

  "processEvent" should "successfully process event end-to-end (download, store, publish)" in {
    val f     = createFixture()
    val event = makeEvent()
    stubSuccessfulFlow(f)

    val result = f.processor.processEvent(event, correlationId).unsafeRunSync()

    val _ = result.isSucceeded shouldBe true
    val _ = result.entityId shouldBe "118-HR-1"
    result shouldBe a[ProcessingResult.Succeeded]
  }

  it should "return Failed when download fails" in {
    val f     = createFixture()
    val event = makeEvent()
    when(f.downloader.download(anyString(), anyString(), any[UUID]))
      .thenReturn(
        IO.raiseError(TextDownloadFailed(event.textUrl, event.textFormat, "HTTP 404", new RuntimeException("404")))
      )

    val result = f.processor.processEvent(event, correlationId).unsafeRunSync()

    val _ = result.isFailed shouldBe true
    val _ = result.entityId shouldBe "118-HR-1"
    val _ = verify(f.repository, never()).insertVersion(any[BillTextVersionDO])
    verify(f.eventPublisher, never()).billTextIngested(any[BillTextIngestedEvent], any[UUID])
  }

  it should "return Failed when DB store fails" in {
    val f     = createFixture()
    val event = makeEvent()
    when(f.downloader.download(anyString(), anyString(), any[UUID])).thenReturn(IO.pure("some content"))
    when(f.repository.insertVersion(any[BillTextVersionDO]))
      .thenReturn(doobie.free.connection.raiseError(new RuntimeException("DB connection lost")))

    val result = f.processor.processEvent(event, correlationId).unsafeRunSync()

    val _ = result.isFailed shouldBe true
    result.entityId shouldBe "118-HR-1"
  }

  it should "return Failed when event publish fails after successful store" in {
    val f     = createFixture()
    val event = makeEvent()
    when(f.downloader.download(anyString(), anyString(), any[UUID])).thenReturn(IO.pure("some content"))
    when(f.repository.insertVersion(any[BillTextVersionDO])).thenReturn(doobie.free.connection.pure(1L))
    when(f.eventPublisher.billTextIngested(any[BillTextIngestedEvent], any[UUID]))
      .thenReturn(IO.raiseError(new RuntimeException("Pub/Sub unavailable")))

    val result = f.processor.processEvent(event, correlationId).unsafeRunSync()

    val _ = result.isFailed shouldBe true
    result.entityId shouldBe "118-HR-1"
  }

  it should "populate BillTextVersionDO with correct fields" in {
    val f = createFixture()
    val event = makeEvent(
      billId = "118-S-42",
      textUrl = "https://api.congress.gov/v3/bill/118/s/42/text/enr",
      textFormat = "Formatted XML",
      versionCode = "enr",
    )
    stubSuccessfulFlow(f, content = "Enrolled bill text")

    val _ = f.processor.processEvent(event, correlationId).unsafeRunSync()

    val captor = ArgumentCaptor.forClass(classOf[BillTextVersionDO])
    val _ = verify(f.repository, times(1)).insertVersion(captor.capture())
    val stored = captor.getValue

    val _ = stored.versionCode shouldBe "enr"
    val _ = stored.versionType shouldBe "Formatted XML"
    val _ = stored.url shouldBe Some("https://api.congress.gov/v3/bill/118/s/42/text/enr")
    val _ = stored.content shouldBe Some("Enrolled bill text")
    val _ = stored.fetchedAt.toString should not be empty
    stored.embedding shouldBe None
  }

  it should "populate BillTextIngestedEvent with correct fields" in {
    val f = createFixture()
    val event = makeEvent(
      billId = "118-HR-99",
      congress = 118,
      versionCode = "rh",
      previousVersionCode = Some("ih"),
    )
    stubSuccessfulFlow(f)

    val _ = f.processor.processEvent(event, correlationId).unsafeRunSync()

    val captor = ArgumentCaptor.forClass(classOf[BillTextIngestedEvent])
    val _ = verify(f.eventPublisher, times(1)).billTextIngested(captor.capture(), any[UUID])
    val published = captor.getValue

    val _ = published.billId shouldBe "118-HR-99"
    val _ = published.congress shouldBe 118
    val _ = published.versionCode shouldBe "rh"
    val _ = published.previousVersionCode shouldBe Some("ih")
    published.committeeCode shouldBe None
  }

  it should "classify IO exceptions as Transient" in {
    val f     = createFixture()
    val event = makeEvent()
    when(f.downloader.download(anyString(), anyString(), any[UUID]))
      .thenReturn(IO.raiseError(new java.io.IOException("network error")))

    val result = f.processor.processEvent(event, correlationId).unsafeRunSync()

    val _ = result.isFailed shouldBe true
    result match {
      case ProcessingResult.Failed(_, _, errorClass) => errorClass shouldBe "Transient"
      case other                                     => fail(s"Expected Failed but got $other")
    }
  }

  it should "classify BillTextProcessingFailed as Systemic" in {
    val f     = createFixture()
    val event = makeEvent()
    when(f.downloader.download(anyString(), anyString(), any[UUID]))
      .thenReturn(IO.raiseError(BillTextProcessingFailed("118-HR-1", "invalid format")))

    val result = f.processor.processEvent(event, correlationId).unsafeRunSync()

    val _ = result.isFailed shouldBe true
    result match {
      case ProcessingResult.Failed(_, _, errorClass) => errorClass shouldBe "Systemic"
      case other                                     => fail(s"Expected Failed but got $other")
    }
  }

  it should "not publish event when download fails" in {
    val f     = createFixture()
    val event = makeEvent()
    when(f.downloader.download(anyString(), anyString(), any[UUID]))
      .thenReturn(IO.raiseError(new RuntimeException("download failed")))

    val _ = f.processor.processEvent(event, correlationId).unsafeRunSync()
    verify(f.eventPublisher, never()).billTextIngested(any[BillTextIngestedEvent], any[UUID])
  }

  it should "log error when processing fails" in {
    val f     = createFixture()
    val event = makeEvent()
    when(f.downloader.download(anyString(), anyString(), any[UUID]))
      .thenReturn(IO.raiseError(new RuntimeException("test failure")))

    val _ = f.processor.processEvent(event, correlationId).unsafeRunSync()
    verify(f.logger, times(1)).error(any[LogContext], anyString(), any[Option[Throwable]])
  }

  it should "set eventEmitted to true on success" in {
    val f     = createFixture()
    val event = makeEvent()
    stubSuccessfulFlow(f)

    val result = f.processor.processEvent(event, correlationId).unsafeRunSync()

    result match {
      case ProcessingResult.Succeeded(_, eventEmitted) => eventEmitted shouldBe true
      case other                                       => fail(s"Expected Succeeded but got $other")
    }
  }

}
