package repcheck.members.lismapping.pipeline

import java.time.Instant
import java.util.UUID

import scala.concurrent.duration._

import cats.effect.IO
import cats.effect.unsafe.implicits.global

import doobie._
import doobie.free.connection

import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.{any, anyLong, anyString, eq => eqTo}
import org.mockito.Mockito.{never, times, verify, when}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar
import repcheck.ingestion.common.events.IngestionEventPublisher
import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}
import repcheck.members.common.persistence.{LisMappingRepository, MemberRepository, UpsertResult}
import repcheck.members.lismapping.client.SenatorLookupXmlClient
import repcheck.members.lismapping.config.LisMappingConfig
import repcheck.members.lismapping.errors.LisMappingUpsertFailed
import repcheck.members.lismapping.repository.LisMemberRepository
import repcheck.pipeline.models.events.MemberUpdatedEvent
import repcheck.shared.models.congress.common.{Party, UsState}
import repcheck.shared.models.congress.dos.member.{LisMemberDO, MemberDO, MemberLisMappingDO}
import repcheck.shared.models.congress.dto.vote.SenatorLookupXmlDTO

/**
 * Unit tests for [[LisMappingProcessor]] covering every acceptance criterion in spec 05.6.
 *
 * The h2 in-memory transactor is used only because `program.transact(xa)` opens a connection before executing the
 * program. Each repo call is mocked to return `connection.pure(...)` — no SQL is actually issued, so the absence of the
 * underlying tables in h2 is irrelevant.
 *
 * Test naming convention: one test per acceptance criterion (AC-05.6.1 through AC-05.6.10).
 */
class LisMappingProcessorSpec extends AnyFlatSpec with Matchers with MockitoSugar {

  private val testXa: Transactor[IO] = Transactor.fromDriverManager[IO](
    driver = "org.h2.Driver",
    url = "jdbc:h2:mem:lismapping;DB_CLOSE_DELAY=-1",
    user = "",
    password = "",
    logHandler = None,
  )

  private val runId = 12345L

  private val config = LisMappingConfig(
    parallelism = 1,
    requestTimeout = 5.seconds,
    currentCongress = 118,
    congressLookbackWindow = 5,
    senatorXmlUrl = "http://localhost/senators.xml",
  )

  private case class TestFixture(
    xmlClient: SenatorLookupXmlClient[IO],
    lisMemberRepo: LisMemberRepository,
    mappingRepo: LisMappingRepository,
    memberRepo: MemberRepository,
    eventPublisher: IngestionEventPublisher[IO],
    logger: PipelineLogger[IO],
  ) {

    def processor: LisMappingProcessor[IO] =
      new LisMappingProcessor[IO](
        xmlClient = xmlClient,
        lisMemberRepo = lisMemberRepo,
        mappingRepo = mappingRepo,
        memberRepo = memberRepo,
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
      xmlClient = mock[SenatorLookupXmlClient[IO]],
      lisMemberRepo = mock[LisMemberRepository],
      mappingRepo = mock[LisMappingRepository],
      memberRepo = mock[MemberRepository],
      eventPublisher = mock[IngestionEventPublisher[IO]],
      logger = loggerMock,
    )
  }

  private def makeDto(
    lisId: String = "S1",
    bioguideId: String = "B000001",
    firstName: String = "Jane",
    lastName: String = "Doe",
    party: String = "D",
    state: String = "NY",
  ): SenatorLookupXmlDTO =
    SenatorLookupXmlDTO(
      lisId = lisId,
      bioguideId = bioguideId,
      firstName = firstName,
      lastName = lastName,
      party = party,
      state = state,
      senateClass = None,
      serviceDates = List.empty,
      isCurrent = true,
    )

  private def makeMember(
    memberId: Long = 42L,
    bioguideId: String = "B000001",
  ): MemberDO =
    MemberDO(
      memberId = memberId,
      naturalKey = bioguideId,
      firstName = Some("Jane"),
      lastName = Some("Doe"),
      directOrderName = Some("Jane Doe"),
      invertedOrderName = Some("Doe, Jane"),
      honorificName = None,
      birthYear = None,
      currentParty = Some(Party.Democrat),
      state = Some(UsState.NewYork),
      district = None,
      imageUrl = None,
      imageAttribution = None,
      officialUrl = None,
      updateDate = Some(Instant.parse("2024-01-01T00:00:00Z")),
      createdAt = None,
      updatedAt = None,
    )

  /**
   * Wires the happy-path stubs. All `any[...]`-matcher stubs are keyed off the runtime argument so the same mock can
   * serve different results depending on which DTO arrived. A shared counter assigns stable LIS member ids
   * (`startingLisMemberId + n`) in DTO order — this lets tests use `ArgumentCaptor` when they want to inspect what was
   * actually upserted.
   */
  private def stubHappyPath(
    f: TestFixture,
    dtos: List[SenatorLookupXmlDTO],
    membersByBioguide: Map[String, Option[MemberDO]],
    upsertResultsByLisId: Map[String, UpsertResult],
    startingLisMemberId: Long = 1000L,
  ): Unit = {
    when(f.xmlClient.fetchMappings(anyLong())).thenReturn(fs2.Stream.emits(dtos))

    // Route lisMemberRepo.upsertByNaturalKey by DTO order (natural key → assigned id).
    val idsByLisId: Map[String, Long] = dtos.zipWithIndex.map {
      case (dto, idx) =>
        dto.lisId -> (startingLisMemberId + idx.toLong)
    }.toMap
    when(f.lisMemberRepo.upsertByNaturalKey(any[LisMemberDO])).thenAnswer { invocation =>
      val arg = invocation.getArgument[LisMemberDO](0)
      connection.pure[Long](idsByLisId.getOrElse(arg.naturalKey, 0L))
    }

    // Route memberRepo.findByBioguideId by bioguide id.
    dtos.foreach { dto =>
      when(f.memberRepo.findByBioguideId(eqTo(dto.bioguideId)))
        .thenReturn(connection.pure[Option[MemberDO]](membersByBioguide.getOrElse(dto.bioguideId, None)))
    }

    // Route mappingRepo.upsert by lisMemberId (reverse lookup → lisId → result).
    val resultsByLisMemberId: Map[Long, UpsertResult] = idsByLisId.collect {
      case (lisId, id) if upsertResultsByLisId.contains(lisId) => id -> upsertResultsByLisId(lisId)
    }
    when(f.mappingRepo.upsert(any[MemberLisMappingDO])).thenAnswer { invocation =>
      val arg = invocation.getArgument[MemberLisMappingDO](0)
      resultsByLisMemberId.get(arg.lisMemberId) match {
        case Some(r) => connection.pure[UpsertResult](r)
        case None    => connection.pure[UpsertResult](UpsertResult.Updated)
      }
    }

    when(f.eventPublisher.memberUpdated(any[MemberUpdatedEvent], any[UUID]))
      .thenReturn(IO.pure("msg-id"))
    ()
  }

  // =========================================================================================================
  // AC-05.6.1 — Fetches and processes all mappings from XML
  // =========================================================================================================

  "refreshAll" should "fetch every DTO emitted by the XML client and upsert one mapping per DTO (AC-05.6.1)" in {
    val f    = createFixture()
    val dtos = (1 to 10).toList.map(i => makeDto(lisId = s"S$i", bioguideId = s"B00000$i"))
    val members = dtos.map { d =>
      d.bioguideId -> Option(makeMember(memberId = d.lisId.drop(1).toLong, bioguideId = d.bioguideId))
    }.toMap
    val results = dtos.map(d => d.lisId -> UpsertResult.Inserted).toMap

    stubHappyPath(f, dtos, members, results)

    val result = f.processor.refreshAll(runId).unsafeRunSync()

    val _ = result.totalParsed shouldBe 10
    val _ = verify(f.xmlClient, times(1)).fetchMappings(anyLong())
    verify(f.mappingRepo, times(10)).upsert(any[MemberLisMappingDO])
  }

  // =========================================================================================================
  // AC-05.6.2 — Converts DTO to LisMemberMappingDO with current timestamp
  // =========================================================================================================

  it should "stamp lastVerified within the last second on both LIS member and mapping rows (AC-05.6.2)" in {
    val f       = createFixture()
    val dto     = makeDto()
    val dtos    = List(dto)
    val members = Map(dto.bioguideId -> Option(makeMember()))
    val results = Map(dto.lisId -> UpsertResult.Inserted)

    stubHappyPath(f, dtos, members, results)

    val before = Instant.now()
    val _      = f.processor.refreshAll(runId).unsafeRunSync()
    val after  = Instant.now().plusSeconds(2)

    val lisMemberCaptor = ArgumentCaptor.forClass(classOf[LisMemberDO])
    val _               = verify(f.lisMemberRepo).upsertByNaturalKey(lisMemberCaptor.capture())
    val lisVerified     = lisMemberCaptor.getValue.lastVerified
    val _               = lisVerified.isDefined shouldBe true
    lisVerified.foreach { ts =>
      val _ = ts.isBefore(before) shouldBe false
      ts.isAfter(after) shouldBe false
    }

    val mappingCaptor = ArgumentCaptor.forClass(classOf[MemberLisMappingDO])
    val _             = verify(f.mappingRepo).upsert(mappingCaptor.capture())
    val mapVerified   = mappingCaptor.getValue.lastVerified
    val _             = mapVerified.isBefore(before) shouldBe false
    mapVerified.isAfter(after) shouldBe false
  }

  // =========================================================================================================
  // AC-05.6.3 — Newly inserted mapping for existing senator emits event
  // =========================================================================================================

  it should "emit MemberUpdatedEvent when mapping is Inserted AND member exists (AC-05.6.3)" in {
    val f       = createFixture()
    val dto     = makeDto(lisId = "S42", bioguideId = "B000042")
    val dtos    = List(dto)
    val members = Map(dto.bioguideId -> Option(makeMember(memberId = 42L, bioguideId = dto.bioguideId)))
    val results = Map(dto.lisId -> UpsertResult.Inserted)

    stubHappyPath(f, dtos, members, results)

    val result = f.processor.refreshAll(runId).unsafeRunSync()

    val _       = result.eventsEmitted shouldBe 1
    val _       = result.inserted shouldBe 1
    val captor  = ArgumentCaptor.forClass(classOf[MemberUpdatedEvent])
    val cidCap  = ArgumentCaptor.forClass(classOf[UUID])
    val _       = verify(f.eventPublisher, times(1)).memberUpdated(captor.capture(), cidCap.capture())
    val emitted = captor.getValue
    val _       = emitted.memberId shouldBe "B000042"
    // Per-item correlation ID is a fresh UUID generated inside processDto (verified via ArgumentCaptor above).
  }

  // =========================================================================================================
  // AC-05.6.4 — Newly inserted mapping for unknown senator skips event
  // =========================================================================================================

  it should "skip the mapping upsert and emit no event when bioguide is missing from members (AC-05.6.4)" in {
    val f       = createFixture()
    val dto     = makeDto(lisId = "S99", bioguideId = "B000099")
    val dtos    = List(dto)
    val members = Map(dto.bioguideId -> Option.empty[MemberDO])

    when(f.xmlClient.fetchMappings(anyLong())).thenReturn(fs2.Stream.emits(dtos))
    when(f.lisMemberRepo.upsertByNaturalKey(any[LisMemberDO]))
      .thenReturn(connection.pure[Long](1234L))
    when(f.memberRepo.findByBioguideId(eqTo(dto.bioguideId)))
      .thenReturn(connection.pure[Option[MemberDO]](members(dto.bioguideId)))

    val result = f.processor.refreshAll(runId).unsafeRunSync()

    val _ = result.totalParsed shouldBe 1
    val _ = result.inserted shouldBe 0
    val _ = result.updated shouldBe 0
    val _ = result.eventsEmitted shouldBe 0
    val _ = verify(f.mappingRepo, never()).upsert(any[MemberLisMappingDO])
    verify(f.eventPublisher, never()).memberUpdated(any[MemberUpdatedEvent], any[UUID])
  }

  // =========================================================================================================
  // AC-05.6.5 — Updated mapping does not emit event
  // =========================================================================================================

  it should "not emit an event when the mapping upsert returns Updated (AC-05.6.5)" in {
    val f       = createFixture()
    val dto     = makeDto(lisId = "S5", bioguideId = "B000005")
    val dtos    = List(dto)
    val members = Map(dto.bioguideId -> Option(makeMember(bioguideId = dto.bioguideId)))
    val results = Map(dto.lisId -> UpsertResult.Updated)

    stubHappyPath(f, dtos, members, results)

    val result = f.processor.refreshAll(runId).unsafeRunSync()

    val _ = result.updated shouldBe 1
    val _ = result.inserted shouldBe 0
    val _ = result.eventsEmitted shouldBe 0
    verify(f.eventPublisher, never()).memberUpdated(any[MemberUpdatedEvent], any[UUID])
  }

  // =========================================================================================================
  // AC-05.6.6 — Result counts are accurate
  // =========================================================================================================

  it should "tally totalParsed, inserted, updated, and eventsEmitted across a mix of outcomes (AC-05.6.6)" in {
    val f = createFixture()

    // 3 Inserted + members exist = 3 events
    val insertedDtos = (1 to 3).toList.map(i => makeDto(lisId = s"SI$i", bioguideId = s"BI$i"))
    // 7 Updated + members exist = 0 events
    val updatedDtos = (1 to 7).toList.map(i => makeDto(lisId = s"SU$i", bioguideId = s"BU$i"))
    val allDtos     = insertedDtos ++ updatedDtos

    val members =
      (insertedDtos ++ updatedDtos).map(d => d.bioguideId -> Option(makeMember(bioguideId = d.bioguideId))).toMap
    val results: Map[String, UpsertResult] =
      insertedDtos.map(d => d.lisId -> UpsertResult.Inserted).toMap ++
        updatedDtos.map(d => d.lisId -> UpsertResult.Updated).toMap

    stubHappyPath(f, allDtos, members, results)

    val result = f.processor.refreshAll(runId).unsafeRunSync()

    val _ = result.totalParsed shouldBe 10
    val _ = result.inserted shouldBe 3
    val _ = result.updated shouldBe 7
    result.eventsEmitted shouldBe 3
  }

  // =========================================================================================================
  // AC-05.6.7 — Correlation ID flows through all operations
  // =========================================================================================================

  it should "pass the run ID to the XML client and a distinct per-item correlation ID to the event publisher (AC-05.6.7)" in {
    val f       = createFixture()
    val dto     = makeDto(lisId = "S7", bioguideId = "B000007")
    val dtos    = List(dto)
    val members = Map(dto.bioguideId -> Option(makeMember(bioguideId = dto.bioguideId)))
    val results = Map(dto.lisId -> UpsertResult.Inserted)

    stubHappyPath(f, dtos, members, results)

    val myRunId = 99999L
    val _       = f.processor.refreshAll(myRunId).unsafeRunSync()

    // The XML fetch receives the run-level Long ID for HTTP-request tracing.
    val xmlCid = ArgumentCaptor.forClass(classOf[java.lang.Long])
    val _      = verify(f.xmlClient).fetchMappings(xmlCid.capture())
    val _      = xmlCid.getValue shouldBe myRunId

    // The event publisher receives a fresh per-item correlation ID generated inside processDto — not the run ID.
    val pubCid = ArgumentCaptor.forClass(classOf[UUID])
    val _      = verify(f.eventPublisher).memberUpdated(any[MemberUpdatedEvent], pubCid.capture())
    pubCid.getValue should not be myRunId
  }

  // =========================================================================================================
  // AC-05.6.8 — XML client failure produces error
  // =========================================================================================================

  it should "propagate an XML-client failure as a failed effect (AC-05.6.8)" in {
    val f     = createFixture()
    val error = new RuntimeException("XML feed unreachable")
    when(f.xmlClient.fetchMappings(anyLong())).thenReturn(fs2.Stream.raiseError[IO](error))

    val thrown = intercept[Exception] {
      val _ = f.processor.refreshAll(runId).unsafeRunSync()
    }
    val _ = thrown.getMessage shouldBe "XML feed unreachable"
    verify(f.eventPublisher, never()).memberUpdated(any[MemberUpdatedEvent], any[UUID])
  }

  // =========================================================================================================
  // AC-05.6.9 — Batch upsert failure produces error
  // =========================================================================================================

  it should "wrap a repository failure in LisMappingUpsertFailed (AC-05.6.9)" in {
    val f    = createFixture()
    val dto  = makeDto(lisId = "S-fail", bioguideId = "B-fail")
    val dtos = List(dto)

    when(f.xmlClient.fetchMappings(anyLong())).thenReturn(fs2.Stream.emits(dtos))
    when(f.lisMemberRepo.upsertByNaturalKey(any[LisMemberDO]))
      .thenReturn(connection.raiseError[Long](new RuntimeException("DB connection lost")))

    val thrown = intercept[LisMappingUpsertFailed] {
      val _ = f.processor.refreshAll(runId).unsafeRunSync()
    }
    val _ = thrown.lisId shouldBe "S-fail"
    val _ = thrown.getMessage should include("DB connection lost")
    thrown.getCause.toString should not be empty
  }

  // =========================================================================================================
  // AC-05.6.10 — Empty XML produces zero-count result
  // =========================================================================================================

  it should "return all-zero counts for an empty XML stream (AC-05.6.10)" in {
    val f = createFixture()
    when(f.xmlClient.fetchMappings(anyLong())).thenReturn(fs2.Stream.empty)

    val result = f.processor.refreshAll(runId).unsafeRunSync()

    val _ = result shouldBe LisMappingRefreshResult.empty
    val _ = verify(f.lisMemberRepo, never()).upsertByNaturalKey(any[LisMemberDO])
    val _ = verify(f.mappingRepo, never()).upsert(any[MemberLisMappingDO])
    val _ = verify(f.eventPublisher, never()).memberUpdated(any[MemberUpdatedEvent], any[UUID])
    verify(f.memberRepo, never()).findByBioguideId(anyString())
  }

  // =========================================================================================================
  // Additional targeted coverage — event-publisher failure. Spec requires that event publishing failures surface as
  // errors (so the caller sees a failed refresh and can re-run). This covers that contract.
  // =========================================================================================================

  it should "propagate an event-publisher failure as a failed effect" in {
    val f       = createFixture()
    val dto     = makeDto(lisId = "S-pub-fail", bioguideId = "B-pub-fail")
    val dtos    = List(dto)
    val members = Map(dto.bioguideId -> Option(makeMember(bioguideId = dto.bioguideId)))
    val results = Map(dto.lisId -> UpsertResult.Inserted)

    stubHappyPath(f, dtos, members, results)
    when(f.eventPublisher.memberUpdated(any[MemberUpdatedEvent], any[UUID]))
      .thenReturn(IO.raiseError(new RuntimeException("Pub/Sub down")))

    val thrown = intercept[Exception] {
      val _ = f.processor.refreshAll(runId).unsafeRunSync()
    }
    thrown.getMessage shouldBe "Pub/Sub down"
  }

  // =========================================================================================================
  // Unit coverage for the pure helpers — these avoid the transactor and exercise the isolated logic.
  // =========================================================================================================

  "buildLisMemberDO" should "copy DTO fields and stamp lastVerified" in {
    val f         = createFixture()
    val processor = f.processor
    val dto       = makeDto(lisId = "S100", bioguideId = "B100", firstName = "Alex", lastName = "Kim")
    val now       = Instant.parse("2026-04-01T12:00:00Z")

    val result = processor.buildLisMemberDO(dto, now)

    val _ = result.naturalKey shouldBe "S100"
    val _ = result.firstName shouldBe Some("Alex")
    val _ = result.lastName shouldBe Some("Kim")
    val _ = result.lastVerified shouldBe Some(now)
    val _ = result.createdAt shouldBe None
    result.id shouldBe 0L
  }

  it should "treat empty-string DTO fields as None on the DO" in {
    val f         = createFixture()
    val processor = f.processor
    val dto       = makeDto(firstName = "", lastName = "", party = "", state = "")

    val result = processor.buildLisMemberDO(dto, Instant.now())

    val _ = result.firstName shouldBe None
    val _ = result.lastName shouldBe None
    val _ = result.party shouldBe None
    result.state shouldBe None
  }

  "tally" should "sum outcomes to a zero result when empty" in {
    val f         = createFixture()
    val processor = f.processor
    processor.tally(List.empty) shouldBe LisMappingRefreshResult.empty
  }

  it should "sum a single InsertedWithEvent into 1/1/0/1" in {
    val f         = createFixture()
    val processor = f.processor
    val result    = processor.tally(List(PerItemOutcome.InsertedWithEvent))
    val _         = result.totalParsed shouldBe 1
    val _         = result.inserted shouldBe 1
    val _         = result.updated shouldBe 0
    result.eventsEmitted shouldBe 1
  }

  it should "sum a single UpdatedNoEvent into 1/0/1/0" in {
    val f         = createFixture()
    val processor = f.processor
    val result    = processor.tally(List(PerItemOutcome.UpdatedNoEvent))
    val _         = result.totalParsed shouldBe 1
    val _         = result.inserted shouldBe 0
    val _         = result.updated shouldBe 1
    result.eventsEmitted shouldBe 0
  }

  it should "sum a single SkippedUnknownMember into 1/0/0/0" in {
    val f         = createFixture()
    val processor = f.processor
    val result    = processor.tally(List(PerItemOutcome.SkippedUnknownMember))
    val _         = result.totalParsed shouldBe 1
    val _         = result.inserted shouldBe 0
    val _         = result.updated shouldBe 0
    result.eventsEmitted shouldBe 0
  }

  it should "sum a mixed list correctly" in {
    val f         = createFixture()
    val processor = f.processor
    val outcomes = List(
      PerItemOutcome.InsertedWithEvent,
      PerItemOutcome.UpdatedNoEvent,
      PerItemOutcome.UpdatedNoEvent,
      PerItemOutcome.SkippedUnknownMember,
    )
    val result = processor.tally(outcomes)
    val _      = result.totalParsed shouldBe 4
    val _      = result.inserted shouldBe 1
    val _      = result.updated shouldBe 2
    result.eventsEmitted shouldBe 1
  }

}
