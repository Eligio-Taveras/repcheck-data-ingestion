package repcheck.ingestion.amendments.textcheck.pipeline

import java.util.UUID

import scala.concurrent.duration._

import cats.effect.IO
import cats.effect.unsafe.implicits.global

import doobie._

import org.mockito.ArgumentMatchers.{any, anyLong, anyString, eq => eqTo}
import org.mockito.Mockito.{never, times, verify, when}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar
import repcheck.ingestion.amendments.persistence.AmendmentRepository
import repcheck.ingestion.amendments.textcheck.api.AmendmentTextApiClient
import repcheck.ingestion.amendments.textcheck.config.AmendmentTextCheckerConfig
import repcheck.ingestion.amendments.textcheck.events.AmendmentTextEventPublisher
import repcheck.ingestion.amendments.textcheck.persistence.AmendmentTextVersionLookup
import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}
import repcheck.pipeline.models.events.AmendmentTextAvailableEvent
import repcheck.shared.models.congress.amendment.AmendmentType
import repcheck.shared.models.congress.common.Chamber
import repcheck.shared.models.congress.dos.amendment.AmendmentDO
import repcheck.shared.models.congress.dto.amendment.{AmendmentFormatDTO, AmendmentTextItemDTO}

class AmendmentTextAvailabilityCheckerSpec extends AnyFlatSpec with Matchers with MockitoSugar {

  // H2 transactor — never actually executes SQL because all repository calls are mocked. Doobie's `.transact(xa)`
  // requires a transactor; mocked ConnectionIOs return pre-built values without ever hitting the driver.
  private val testXa: Transactor[IO] = Transactor.fromDriverManager[IO](
    driver = "org.h2.Driver",
    url = "jdbc:h2:mem:amendmenttextcheck;DB_CLOSE_DELAY=-1",
    user = "",
    password = "",
    logHandler = None,
  )

  private val correlationId = UUID.fromString("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
  private val runId         = 4242L

  private val baseConfig = AmendmentTextCheckerConfig(
    minCongress = 117,
    staleAfter = 1.hour,
    parallelism = 1,
    pageDelay = 0.millis,
    pageSize = 250,
  )

  private case class TestFixture(
    apiClient: AmendmentTextApiClient[IO],
    amendmentRepo: AmendmentRepository[ConnectionIO],
    textVersionLookup: AmendmentTextVersionLookup[ConnectionIO],
    eventPublisher: AmendmentTextEventPublisher[IO],
    logger: PipelineLogger[IO],
  ) {

    def checker: AmendmentTextAvailabilityChecker[IO] =
      new AmendmentTextAvailabilityChecker[IO](
        apiClient = apiClient,
        amendmentRepo = amendmentRepo,
        textVersionLookup = textVersionLookup,
        eventPublisher = eventPublisher,
        xa = testXa,
        config = baseConfig,
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
      apiClient = mock[AmendmentTextApiClient[IO]],
      amendmentRepo = mock[AmendmentRepository[ConnectionIO]],
      textVersionLookup = mock[AmendmentTextVersionLookup[ConnectionIO]],
      eventPublisher = mock[AmendmentTextEventPublisher[IO]],
      logger = loggerMock,
    )
  }

  private def makeAmendment(
    amendmentId: Long = 1L,
    naturalKey: String = "117-SAMDT-2137",
    congress: Int = 117,
    amendmentType: AmendmentType = AmendmentType.SAMDT,
    number: String = "2137",
    chamber: Chamber = Chamber.Senate,
  ): AmendmentDO =
    AmendmentDO(
      amendmentId = amendmentId,
      naturalKey = naturalKey,
      congress = congress,
      amendmentType = Some(amendmentType),
      number = number,
      billId = None,
      chamber = chamber,
      description = None,
      purpose = None,
      sponsorMemberId = None,
      submittedDate = None,
      proposedDate = None,
      latestActionDate = None,
      latestActionTime = None,
      latestActionText = None,
      updateDate = None,
      apiUrl = None,
      parentAmendmentId = None,
      lastTextCheckAt = None,
      createdAt = None,
      updatedAt = None,
    )

  private def submittedHtml(): AmendmentTextItemDTO =
    AmendmentTextItemDTO(
      `type` = Some("Submitted"),
      date = Some("2024-04-01T12:00:00Z"),
      formats = List(AmendmentFormatDTO("HTML", "https://www.congress.gov/sub.htm")),
    )

  private def modifiedPdf(): AmendmentTextItemDTO =
    AmendmentTextItemDTO(
      `type` = Some("Modified"),
      date = Some("2024-04-15T12:00:00Z"),
      formats = List(AmendmentFormatDTO("PDF", "https://www.congress.gov/mod.pdf")),
    )

  "checkAmendment" should "emit one event per new tuple and stamp last_text_check_at on success" in {
    val f         = createFixture()
    val amendment = makeAmendment()

    when(f.apiClient.fetchTextVersions(any[Int], any[AmendmentType], anyString(), any[UUID]))
      .thenReturn(IO.pure(List(submittedHtml(), modifiedPdf())))
    when(f.textVersionLookup.findExistingVersions(anyLong()))
      .thenReturn(doobie.free.connection.pure(List.empty[(String, String)]))
    when(f.eventPublisher.publish(any[AmendmentTextAvailableEvent], any[UUID]))
      .thenReturn(IO.pure("msg-id"))
    when(f.amendmentRepo.updateLastTextCheckAt(anyLong()))
      .thenReturn(doobie.free.connection.pure(()))

    val result = f.checker.checkAmendment(amendment, correlationId).unsafeRunSync()
    val _      = result.isSucceeded shouldBe true
    val _      = result.entityId shouldBe "117-SAMDT-2137"
    val _      = verify(f.eventPublisher, times(2)).publish(any[AmendmentTextAvailableEvent], any[UUID])
    verify(f.amendmentRepo, times(1)).updateLastTextCheckAt(eqTo(1L))
  }

  it should "emit zero events and return Skipped when all upstream tuples are already ingested" in {
    val f         = createFixture()
    val amendment = makeAmendment()

    when(f.apiClient.fetchTextVersions(any[Int], any[AmendmentType], anyString(), any[UUID]))
      .thenReturn(IO.pure(List(submittedHtml())))
    when(f.textVersionLookup.findExistingVersions(anyLong()))
      .thenReturn(doobie.free.connection.pure(List(("SUB", "HTML"))))
    when(f.amendmentRepo.updateLastTextCheckAt(anyLong()))
      .thenReturn(doobie.free.connection.pure(()))

    val result = f.checker.checkAmendment(amendment, correlationId).unsafeRunSync()
    val _      = result.isSkipped shouldBe true
    val _      = verify(f.eventPublisher, never()).publish(any[AmendmentTextAvailableEvent], any[UUID])
    // Skipped still counts as "successfully reached upstream" — stamp last_text_check_at.
    verify(f.amendmentRepo, times(1)).updateLastTextCheckAt(eqTo(1L))
  }

  it should "emit zero events and return Skipped when upstream returns empty" in {
    val f         = createFixture()
    val amendment = makeAmendment()

    when(f.apiClient.fetchTextVersions(any[Int], any[AmendmentType], anyString(), any[UUID]))
      .thenReturn(IO.pure(List.empty))
    when(f.textVersionLookup.findExistingVersions(anyLong()))
      .thenReturn(doobie.free.connection.pure(List.empty[(String, String)]))
    when(f.amendmentRepo.updateLastTextCheckAt(anyLong()))
      .thenReturn(doobie.free.connection.pure(()))

    val result = f.checker.checkAmendment(amendment, correlationId).unsafeRunSync()
    val _      = result.isSkipped shouldBe true
    val _      = verify(f.eventPublisher, never()).publish(any[AmendmentTextAvailableEvent], any[UUID])
    verify(f.amendmentRepo, times(1)).updateLastTextCheckAt(eqTo(1L))
  }

  it should "NOT stamp last_text_check_at when the API call fails" in {
    val f         = createFixture()
    val amendment = makeAmendment()

    when(f.apiClient.fetchTextVersions(any[Int], any[AmendmentType], anyString(), any[UUID]))
      .thenReturn(IO.raiseError(new RuntimeException("API unavailable")))

    val result = f.checker.checkAmendment(amendment, correlationId).unsafeRunSync()
    val _      = result.isFailed shouldBe true
    verify(f.amendmentRepo, never()).updateLastTextCheckAt(anyLong())
  }

  it should "NOT stamp last_text_check_at when publishing fails" in {
    val f         = createFixture()
    val amendment = makeAmendment()

    when(f.apiClient.fetchTextVersions(any[Int], any[AmendmentType], anyString(), any[UUID]))
      .thenReturn(IO.pure(List(submittedHtml())))
    when(f.textVersionLookup.findExistingVersions(anyLong()))
      .thenReturn(doobie.free.connection.pure(List.empty[(String, String)]))
    when(f.eventPublisher.publish(any[AmendmentTextAvailableEvent], any[UUID]))
      .thenReturn(IO.raiseError(new RuntimeException("Pub/Sub down")))

    val result = f.checker.checkAmendment(amendment, correlationId).unsafeRunSync()
    val _      = result.isFailed shouldBe true
    verify(f.amendmentRepo, never()).updateLastTextCheckAt(anyLong())
  }

  it should "populate event fields from the amendment + upstream tuple" in {
    val f = createFixture()
    val amendment = makeAmendment(
      amendmentId = 99L,
      naturalKey = "118-HAMDT-5",
      congress = 118,
      amendmentType = AmendmentType.HAMDT,
      number = "5",
      chamber = Chamber.House,
    )

    when(f.apiClient.fetchTextVersions(any[Int], any[AmendmentType], anyString(), any[UUID]))
      .thenReturn(IO.pure(List(submittedHtml())))
    when(f.textVersionLookup.findExistingVersions(anyLong()))
      .thenReturn(doobie.free.connection.pure(List.empty[(String, String)]))
    when(f.eventPublisher.publish(any[AmendmentTextAvailableEvent], any[UUID]))
      .thenReturn(IO.pure("msg-id"))
    when(f.amendmentRepo.updateLastTextCheckAt(anyLong()))
      .thenReturn(doobie.free.connection.pure(()))

    val _ = f.checker.checkAmendment(amendment, correlationId).unsafeRunSync()

    val captor = org.mockito.ArgumentCaptor.forClass(classOf[AmendmentTextAvailableEvent])
    val _      = verify(f.eventPublisher).publish(captor.capture(), any[UUID])
    val event  = captor.getValue
    val _      = event.amendmentId shouldBe 99L
    val _      = event.naturalKey shouldBe "118-HAMDT-5"
    val _      = event.congress shouldBe 118
    val _      = event.amendmentType shouldBe AmendmentType.HAMDT
    val _      = event.number shouldBe "5"
    val _      = event.versionTypeCode shouldBe "SUB"
    val _      = event.formatType shouldBe "HTML"
    val _      = event.url shouldBe "https://www.congress.gov/sub.htm"
    val _      = event.publishedDate.isDefined shouldBe true
    event.correlationId shouldBe correlationId
  }

  it should "leave publishedDate=None when the upstream date string is unparseable" in {
    val f         = createFixture()
    val amendment = makeAmendment()
    val item = AmendmentTextItemDTO(
      `type` = Some("Submitted"),
      date = Some("not-a-date"),
      formats = List(AmendmentFormatDTO("HTML", "https://example.com/foo")),
    )

    when(f.apiClient.fetchTextVersions(any[Int], any[AmendmentType], anyString(), any[UUID]))
      .thenReturn(IO.pure(List(item)))
    when(f.textVersionLookup.findExistingVersions(anyLong()))
      .thenReturn(doobie.free.connection.pure(List.empty[(String, String)]))
    when(f.eventPublisher.publish(any[AmendmentTextAvailableEvent], any[UUID]))
      .thenReturn(IO.pure("msg-id"))
    when(f.amendmentRepo.updateLastTextCheckAt(anyLong()))
      .thenReturn(doobie.free.connection.pure(()))

    val _      = f.checker.checkAmendment(amendment, correlationId).unsafeRunSync()
    val captor = org.mockito.ArgumentCaptor.forClass(classOf[AmendmentTextAvailableEvent])
    val _      = verify(f.eventPublisher).publish(captor.capture(), any[UUID])
    captor.getValue.publishedDate shouldBe None
  }

  it should "raise when the amendment has no amendment_type populated" in {
    val f         = createFixture()
    val amendment = makeAmendment().copy(amendmentType = None)

    val result = f.checker.checkAmendment(amendment, correlationId).unsafeRunSync()
    val _      = result.isFailed shouldBe true
    verify(f.amendmentRepo, never()).updateLastTextCheckAt(anyLong())
  }

  "checkAll" should "emit one ProcessingResult per candidate" in {
    val f = createFixture()
    val candidates = List(
      makeAmendment(amendmentId = 1L, naturalKey = "117-SAMDT-1", number = "1"),
      makeAmendment(amendmentId = 2L, naturalKey = "117-SAMDT-2", number = "2"),
    )

    when(f.amendmentRepo.findCandidatesForTextCheck(any[Int], any[FiniteDuration]))
      .thenReturn(doobie.free.connection.pure(candidates))
    when(f.apiClient.fetchTextVersions(any[Int], any[AmendmentType], anyString(), any[UUID]))
      .thenReturn(IO.pure(List.empty))
    when(f.textVersionLookup.findExistingVersions(anyLong()))
      .thenReturn(doobie.free.connection.pure(List.empty[(String, String)]))
    when(f.amendmentRepo.updateLastTextCheckAt(anyLong()))
      .thenReturn(doobie.free.connection.pure(()))

    val results = f.checker.checkAll(runId).compile.toList.unsafeRunSync()
    val _       = results.size shouldBe 2
    results.foreach(_.isSkipped shouldBe true)
  }

  it should "complete with empty stream when no candidates exist" in {
    val f = createFixture()
    when(f.amendmentRepo.findCandidatesForTextCheck(any[Int], any[FiniteDuration]))
      .thenReturn(doobie.free.connection.pure(List.empty[AmendmentDO]))

    val results = f.checker.checkAll(runId).compile.toList.unsafeRunSync()
    results shouldBe empty
  }

  it should "not halt the stream when one candidate fails" in {
    val f = createFixture()
    val candidates = List(
      makeAmendment(amendmentId = 1L, naturalKey = "117-SAMDT-1", number = "1"),
      makeAmendment(amendmentId = 2L, naturalKey = "117-SAMDT-2", number = "2"),
    )

    when(f.amendmentRepo.findCandidatesForTextCheck(any[Int], any[FiniteDuration]))
      .thenReturn(doobie.free.connection.pure(candidates))
    // First candidate raises, second succeeds (empty upstream).
    when(f.apiClient.fetchTextVersions(eqTo(117), any[AmendmentType], eqTo("1"), any[UUID]))
      .thenReturn(IO.raiseError(new RuntimeException("boom")))
    when(f.apiClient.fetchTextVersions(eqTo(117), any[AmendmentType], eqTo("2"), any[UUID]))
      .thenReturn(IO.pure(List.empty))
    when(f.textVersionLookup.findExistingVersions(anyLong()))
      .thenReturn(doobie.free.connection.pure(List.empty[(String, String)]))
    when(f.amendmentRepo.updateLastTextCheckAt(anyLong()))
      .thenReturn(doobie.free.connection.pure(()))

    val results = f.checker.checkAll(runId).compile.toList.unsafeRunSync()
    val _       = results.size shouldBe 2
    val _       = results.count(_.isFailed) shouldBe 1
    results.count(_.isSkipped) shouldBe 1
  }

  it should "use a fresh correlationId per candidate" in {
    val f = createFixture()
    val candidates = List(
      makeAmendment(amendmentId = 1L, naturalKey = "117-SAMDT-1", number = "1"),
      makeAmendment(amendmentId = 2L, naturalKey = "117-SAMDT-2", number = "2"),
    )

    when(f.amendmentRepo.findCandidatesForTextCheck(any[Int], any[FiniteDuration]))
      .thenReturn(doobie.free.connection.pure(candidates))
    when(f.apiClient.fetchTextVersions(any[Int], any[AmendmentType], anyString(), any[UUID]))
      .thenReturn(IO.pure(List(submittedHtml())))
    when(f.textVersionLookup.findExistingVersions(anyLong()))
      .thenReturn(doobie.free.connection.pure(List.empty[(String, String)]))
    when(f.eventPublisher.publish(any[AmendmentTextAvailableEvent], any[UUID]))
      .thenReturn(IO.pure("msg-id"))
    when(f.amendmentRepo.updateLastTextCheckAt(anyLong()))
      .thenReturn(doobie.free.connection.pure(()))

    val _ = f.checker.checkAll(runId).compile.toList.unsafeRunSync()

    val captor         = org.mockito.ArgumentCaptor.forClass(classOf[AmendmentTextAvailableEvent])
    val _              = verify(f.eventPublisher, times(2)).publish(captor.capture(), any[UUID])
    val correlationIds = captor.getAllValues.toArray(Array.empty[Object]).toSet
    correlationIds.size shouldBe 2
  }

  "stampLastChecked" should "delegate to the repository" in {
    val f = createFixture()
    when(f.amendmentRepo.updateLastTextCheckAt(anyLong()))
      .thenReturn(doobie.free.connection.pure(()))

    f.checker.stampLastChecked(42L).unsafeRunSync()
    verify(f.amendmentRepo, times(1)).updateLastTextCheckAt(eqTo(42L))
  }

  "parsePublishedDate" should "parse a valid ISO-8601 instant string" in {
    val f = createFixture()
    f.checker.parsePublishedDate("2024-04-01T12:00:00Z").isDefined shouldBe true
  }

  it should "return None for an invalid string" in {
    val f = createFixture()
    f.checker.parsePublishedDate("not-a-date") shouldBe None
  }

}
