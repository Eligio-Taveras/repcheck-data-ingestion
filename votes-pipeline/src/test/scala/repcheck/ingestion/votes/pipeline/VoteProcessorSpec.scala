package repcheck.ingestion.votes.pipeline

import java.time.{Instant, LocalDate}
import java.util.UUID

import cats.effect.IO
import cats.effect.unsafe.implicits.global

import doobie._
import doobie.free.connection

import difflicious.DiffResult
import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.{any, anyLong, anyString, eq => eqTo}
import org.mockito.Mockito.{never, times, verify, when}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar
import repcheck.ingestion.common.events.IngestionEventPublisher
import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}
import repcheck.ingestion.votes.api.HouseVotesApiClient
import repcheck.ingestion.votes.config.{HouseVotesConfig, SenateVoteXmlConfig}
import repcheck.ingestion.votes.lis.LisResolver
import repcheck.ingestion.votes.repo.StanceMaterializationStatusRepository
import repcheck.ingestion.votes.xml.{SenateVoteIndexEntry, SenateVoteXmlClient}
import repcheck.pipeline.models.events.VoteRecordedEvent
import repcheck.pipeline.models.metadata.ProcessingResult
import repcheck.shared.models.congress.common.{BillType, Chamber, Party, UsState}
import repcheck.shared.models.congress.dos.vote.{VoteDO, VotePositionDO}
import repcheck.shared.models.congress.vote.{VoteCast, VoteMethod, VoteType}

/**
 * Unit spec for [[VoteProcessor]]. All collaborators (API clients, resolvers, converters, detector, persister, event
 * publisher, stance repo, transactor) are mocked via MockitoScala; the spec exercises the processor's orchestration
 * without touching any real HTTP or DB infrastructure.
 *
 * Primary focus: `processVote` (the common tail) — every [[VoteChangeReport]] branch, the stance-mark gate driven by
 * `billId.isDefined`, and the event emission / skip semantics.
 *
 * Secondary focus: `streamAll` chamber- and per-vote-level failure isolation via `Stream.handleErrorWith`.
 */
class VoteProcessorSpec extends AnyFlatSpec with Matchers with MockitoSugar {

  private val correlationId: UUID = UUID.fromString("33333333-4444-5555-6666-777777777777")

  private val testXa: Transactor[IO] = Transactor.fromDriverManager[IO](
    driver = "org.h2.Driver",
    url = "jdbc:h2:mem:voteprocessor;DB_CLOSE_DELAY=-1",
    user = "",
    password = "",
    logHandler = None,
  )

  // ------------------------------------------------------------------
  // Fixtures
  // ------------------------------------------------------------------

  private def houseConfig(p: Int = 1)  = HouseVotesConfig(congress = 119, session = 1, parallelism = p)
  private def senateConfig(p: Int = 1) = SenateVoteXmlConfig(parallelism = p)

  private def voteDO(
    voteId: Long = 0L,
    naturalKey: String = "119-House-1-42",
    chamber: Chamber = Chamber.House,
    billId: Option[Long] = Some(100L),
    updateDate: Option[Instant] = Some(Instant.parse("2024-06-01T12:00:00Z")),
  ): VoteDO =
    VoteDO(
      voteId = voteId,
      naturalKey = naturalKey,
      congress = 119,
      chamber = chamber,
      rollNumber = 42,
      sessionNumber = Some(1),
      billId = billId,
      question = Some("On Passage"),
      voteType = Some(VoteType.Passage),
      voteMethod = Some(VoteMethod.RecordedVote),
      result = Some("Passed"),
      voteDate = Some(LocalDate.parse("2024-05-30")),
      legislationNumber = Some("1234"),
      legislationType = Some(BillType.HR),
      legislationUrl = None,
      sourceDataUrl = None,
      updateDate = updateDate,
      createdAt = None,
      updatedAt = None,
    )

  private def pos(memberId: Long): VotePositionDO =
    VotePositionDO(
      id = 0L,
      voteId = 0L,
      memberId = Some(memberId),
      position = Some(VoteCast.Yea),
      partyAtVote = Some(Party.Democrat),
      stateAtVote = Some(UsState.NewYork),
      createdAt = None,
      lisMemberId = None,
    )

  private def mkLogger: PipelineLogger[IO] = {
    val m = mock[PipelineLogger[IO]]
    when(m.info(any[LogContext], anyString())).thenReturn(IO.unit)
    when(m.warn(any[LogContext], anyString())).thenReturn(IO.unit)
    when(m.error(any[LogContext], anyString(), any[Option[Throwable]])).thenReturn(IO.unit)
    m
  }

  /**
   * Holder for every mocked collaborator — keeps the arg list out of each test.
   */
  final private case class ProcessorMocks(
    houseClient: HouseVotesApiClient[IO],
    senateClient: SenateVoteXmlClient[IO],
    lisResolver: LisResolver[IO],
    houseConverter: HouseVoteConverter[IO],
    senateConverter: SenateVoteConverter[IO],
    changeDetector: VoteChangeDetector[IO],
    persister: VotePersister[IO],
    stanceRepo: StanceMaterializationStatusRepository,
    eventPublisher: IngestionEventPublisher[IO],
    findStoredVote: String => IO[Option[VoteDO]],
    logger: PipelineLogger[IO],
  ) {

    def build: VoteProcessor[IO] = new VoteProcessor[IO](
      houseClient = houseClient,
      senateClient = senateClient,
      lisResolver = lisResolver,
      houseConverter = houseConverter,
      senateConverter = senateConverter,
      changeDetector = changeDetector,
      persister = persister,
      stanceRepo = stanceRepo,
      eventPublisher = eventPublisher,
      findStoredVote = findStoredVote,
      xa = testXa,
      houseConfig = houseConfig(),
      senateConfig = senateConfig(),
      congress = 119,
      session = 1,
      logger = mkLogger,
    )

  }

  private def mkMocks(): ProcessorMocks = {
    val mocks = ProcessorMocks(
      houseClient = mock[HouseVotesApiClient[IO]],
      senateClient = mock[SenateVoteXmlClient[IO]],
      lisResolver = mock[LisResolver[IO]],
      houseConverter = mock[HouseVoteConverter[IO]],
      senateConverter = mock[SenateVoteConverter[IO]],
      changeDetector = mock[VoteChangeDetector[IO]],
      persister = mock[VotePersister[IO]],
      stanceRepo = mock[StanceMaterializationStatusRepository],
      eventPublisher = mock[IngestionEventPublisher[IO]],
      findStoredVote = mock[String => IO[Option[VoteDO]]],
      logger = mkLogger,
    )
    // Sensible defaults — tests override as needed.
    when(mocks.stanceRepo.markHasVotes(anyLong())).thenReturn(connection.pure(()))
    when(mocks.eventPublisher.voteRecorded(any[VoteRecordedEvent], any[UUID])).thenReturn(IO.pure("msg-id"))
    mocks
  }

  // ------------------------------------------------------------------
  // processVote — decision matrix
  // ------------------------------------------------------------------

  "processVote" should "persist New + mark stance (bill-linked) + emit VoteRecordedEvent(isUpdate=false) and return Succeeded(eventEmitted=true)" in {
    val mocks = mkMocks()
    val vote  = voteDO(billId = Some(100L))
    val ps    = List(pos(1L))

    when(mocks.findStoredVote.apply(anyString())).thenReturn(IO.pure(Option.empty[VoteDO]))
    when(mocks.changeDetector.detect(any[VoteDO], any[List[VotePositionDO]], any[UUID]))
      .thenReturn(IO.pure(VoteChangeReport.New))
    when(mocks.persister.persistNew(any[VoteDO], any[List[VotePositionDO]]))
      .thenReturn(IO.pure(vote.copy(voteId = 42L)))

    val result = mocks.build
      .processVote(vote, ps, billNaturalKey = Some("119-HR-1234"), correlationId, LogContext("r", "s"))
      .unsafeRunSync()

    val _           = result shouldBe ProcessingResult.Succeeded(vote.naturalKey, eventEmitted = true)
    val _           = verify(mocks.persister, times(1)).persistNew(eqTo(vote), eqTo(ps))
    val _           = verify(mocks.persister, never()).persistUpdate(any[VoteDO], any[List[VotePositionDO]], anyLong())
    val _           = verify(mocks.persister, never()).persistMetadataOnlyUpdate(any[VoteDO], anyLong())
    val _           = verify(mocks.stanceRepo, times(1)).markHasVotes(100L)
    val eventCaptor = ArgumentCaptor.forClass(classOf[VoteRecordedEvent])
    val _           = verify(mocks.eventPublisher, times(1)).voteRecorded(eventCaptor.capture(), any[UUID])
    val event       = eventCaptor.getValue
    val _           = event.isUpdate shouldBe false
    val _           = event.billNaturalKey shouldBe Some("119-HR-1234")
    event.voteNaturalKey shouldBe vote.naturalKey
  }

  it should "persist Updated(positionsChanged=true) + mark stance + emit VoteRecordedEvent(isUpdate=true) and return Succeeded(eventEmitted=true)" in {
    val mocks  = mkMocks()
    val vote   = voteDO(billId = Some(200L))
    val ps     = List(pos(1L))
    val stored = vote.copy(voteId = 77L)

    when(mocks.findStoredVote.apply(anyString())).thenReturn(IO.pure(Some(stored)))
    val diff = DiffResult.ValueResult.Both("a", "b", isSame = false, isIgnored = false)
    when(mocks.changeDetector.detect(any[VoteDO], any[List[VotePositionDO]], any[UUID]))
      .thenReturn(IO.pure(VoteChangeReport.Updated(positionsChanged = true, diff = diff)))
    when(mocks.persister.persistUpdate(any[VoteDO], any[List[VotePositionDO]], anyLong()))
      .thenReturn(IO.pure(stored.copy(voteId = 42L)))

    val result = mocks.build
      .processVote(vote, ps, billNaturalKey = Some("119-HR-9999"), correlationId, LogContext("r", "s"))
      .unsafeRunSync()

    val _ = result shouldBe ProcessingResult.Succeeded(vote.naturalKey, eventEmitted = true)
    // persistUpdate was called with the stored voteId (77L)
    val _           = verify(mocks.persister, times(1)).persistUpdate(eqTo(vote), eqTo(ps), eqTo(77L))
    val _           = verify(mocks.stanceRepo, times(1)).markHasVotes(200L)
    val eventCaptor = ArgumentCaptor.forClass(classOf[VoteRecordedEvent])
    val _           = verify(mocks.eventPublisher, times(1)).voteRecorded(eventCaptor.capture(), any[UUID])
    eventCaptor.getValue.isUpdate shouldBe true
  }

  it should "persist Updated(positionsChanged=false) via persistMetadataOnlyUpdate, SKIP stance mark and SKIP event, return Succeeded(eventEmitted=false)" in {
    val mocks  = mkMocks()
    val vote   = voteDO(billId = Some(300L))
    val ps     = List(pos(1L))
    val stored = vote.copy(voteId = 88L)

    when(mocks.findStoredVote.apply(anyString())).thenReturn(IO.pure(Some(stored)))
    val diff = DiffResult.ValueResult.Both("x", "x", isSame = true, isIgnored = false)
    when(mocks.changeDetector.detect(any[VoteDO], any[List[VotePositionDO]], any[UUID]))
      .thenReturn(IO.pure(VoteChangeReport.Updated(positionsChanged = false, diff = diff)))
    when(mocks.persister.persistMetadataOnlyUpdate(any[VoteDO], anyLong()))
      .thenReturn(IO.pure(stored.copy(voteId = 42L)))

    val result = mocks.build
      .processVote(vote, ps, billNaturalKey = Some("119-HR-1234"), correlationId, LogContext("r", "s"))
      .unsafeRunSync()

    val _ = result shouldBe ProcessingResult.Succeeded(vote.naturalKey, eventEmitted = false)
    val _ = verify(mocks.persister, times(1)).persistMetadataOnlyUpdate(eqTo(vote), eqTo(88L))
    val _ = verify(mocks.persister, never()).persistNew(any[VoteDO], any[List[VotePositionDO]])
    val _ = verify(mocks.persister, never()).persistUpdate(any[VoteDO], any[List[VotePositionDO]], anyLong())
    val _ = verify(mocks.stanceRepo, never()).markHasVotes(anyLong())
    verify(mocks.eventPublisher, never()).voteRecorded(any[VoteRecordedEvent], any[UUID])
  }

  it should "no-op on Unchanged and return Skipped(reason=unchanged)" in {
    val mocks = mkMocks()
    val vote  = voteDO()
    val ps    = List(pos(1L))

    when(mocks.findStoredVote.apply(anyString())).thenReturn(IO.pure(Some(vote.copy(voteId = 99L))))
    when(mocks.changeDetector.detect(any[VoteDO], any[List[VotePositionDO]], any[UUID]))
      .thenReturn(IO.pure(VoteChangeReport.Unchanged))

    val result = mocks.build
      .processVote(vote, ps, billNaturalKey = Some("119-HR-1234"), correlationId, LogContext("r", "s"))
      .unsafeRunSync()

    val _ = result shouldBe ProcessingResult.Skipped(vote.naturalKey, "unchanged")
    val _ = verify(mocks.persister, never()).persistNew(any[VoteDO], any[List[VotePositionDO]])
    val _ = verify(mocks.persister, never()).persistUpdate(any[VoteDO], any[List[VotePositionDO]], anyLong())
    val _ = verify(mocks.persister, never()).persistMetadataOnlyUpdate(any[VoteDO], anyLong())
    val _ = verify(mocks.stanceRepo, never()).markHasVotes(anyLong())
    verify(mocks.eventPublisher, never()).voteRecorded(any[VoteRecordedEvent], any[UUID])
  }

  // ------------------------------------------------------------------
  // Procedural (billId = None) — persist + event, skip stance mark only
  // ------------------------------------------------------------------

  it should "emit VoteRecordedEvent with billNaturalKey=None for procedural votes but SKIP stance mark (schema-driven)" in {
    val mocks = mkMocks()
    val vote  = voteDO(billId = None)
    val ps    = List(pos(1L))

    when(mocks.findStoredVote.apply(anyString())).thenReturn(IO.pure(Option.empty[VoteDO]))
    when(mocks.changeDetector.detect(any[VoteDO], any[List[VotePositionDO]], any[UUID]))
      .thenReturn(IO.pure(VoteChangeReport.New))
    when(mocks.persister.persistNew(any[VoteDO], any[List[VotePositionDO]]))
      .thenReturn(IO.pure(vote.copy(voteId = 42L)))

    val result = mocks.build
      .processVote(vote, ps, billNaturalKey = None, correlationId, LogContext("r", "s"))
      .unsafeRunSync()

    val _           = result shouldBe ProcessingResult.Succeeded(vote.naturalKey, eventEmitted = true)
    val _           = verify(mocks.stanceRepo, never()).markHasVotes(anyLong())
    val eventCaptor = ArgumentCaptor.forClass(classOf[VoteRecordedEvent])
    val _           = verify(mocks.eventPublisher, times(1)).voteRecorded(eventCaptor.capture(), any[UUID])
    val _           = eventCaptor.getValue.billNaturalKey shouldBe None
    eventCaptor.getValue.chamber shouldBe "House"
  }

  // ------------------------------------------------------------------
  // Chamber-level failure isolation
  // ------------------------------------------------------------------

  "streamAll" should "emit a chamber-level ProcessingResult.Failed when fetchRecentVotes raises and the other chamber still runs" in {
    val mocks = mkMocks()
    // House fails at the list endpoint
    when(mocks.houseClient.fetchRecentVotes).thenReturn(IO.raiseError(new RuntimeException("house 401")))
    // Senate returns an empty index — no work, but stream completes cleanly
    when(mocks.senateClient.fetchVoteIndex(eqTo(119), eqTo(1)))
      .thenReturn(IO.pure(List.empty[SenateVoteIndexEntry]))

    val results = mocks.build.streamAll("run-1").compile.toList.unsafeRunSync()

    val houseFailures = results.collect {
      case f @ ProcessingResult.Failed(id, _, _) if id == "house-chamber" => f
    }
    val _ = houseFailures.length shouldBe 1
    houseFailures.headOption.foreach { f =>
      val _ = f.reason should include("house 401")
    }
    // Senate produces no results (empty index) but the stream did NOT abort
    val senateFailures = results.collect {
      case f @ ProcessingResult.Failed(id, _, _) if id == "senate-chamber" => f
    }
    senateFailures shouldBe List.empty
  }

  it should "emit a chamber-level ProcessingResult.Failed when fetchVoteIndex raises (mirror of the house case)" in {
    val mocks = mkMocks()
    when(mocks.houseClient.fetchRecentVotes).thenReturn(IO.pure(List.empty))
    when(mocks.senateClient.fetchVoteIndex(eqTo(119), eqTo(1)))
      .thenReturn(IO.raiseError(new RuntimeException("senate index decode failed")))

    val results = mocks.build.streamAll("run-1").compile.toList.unsafeRunSync()

    val senateFailures = results.collect {
      case f @ ProcessingResult.Failed(id, _, _) if id == "senate-chamber" => f
    }
    val _ = senateFailures.length shouldBe 1
    senateFailures.headOption.foreach(f => f.reason should include("senate index decode failed"))
  }

  // ------------------------------------------------------------------
  // End-to-end-through-the-processor: House list → fetch members → convert → process
  // ------------------------------------------------------------------

  it should "process a single House vote end-to-end: fetch list → fetch members → convert → Succeeded" in {
    val mocks = mkMocks()

    val listItem = repcheck.shared.models.congress.dto.vote.VoteListItemDTO(
      congress = 119,
      chamber = "House",
      rollCallNumber = 42,
      sessionNumber = Some(1),
      startDate = Some("2024-06-01T12:00:00Z"),
      updateDate = Some("2024-06-01T12:00:00Z"),
      result = Some("Passed"),
      voteType = Some("Yea-and-Nay"),
      legislationNumber = Some("1234"),
      legislationType = Some("HR"),
      legislationUrl = None,
      url = None,
      identifier = None,
      sourceDataUrl = None,
    )
    val membersDto = repcheck.shared.models.congress.dto.vote.VoteMembersDTO(
      congress = 119,
      chamber = "House",
      rollCallNumber = 42,
      sessionNumber = Some(1),
      startDate = Some("2024-06-01T12:00:00Z"),
      updateDate = Some("2024-06-01T12:00:00Z"),
      result = Some("Passed"),
      voteType = Some("Yea-and-Nay"),
      legislationNumber = Some("1234"),
      legislationType = Some("HR"),
      legislationUrl = None,
      url = None,
      identifier = None,
      sourceDataUrl = None,
      voteQuestion = Some("On Passage"),
      results = Some(List.empty),
    )
    val vote = voteDO(naturalKey = "119-House-1-42")
    val ps   = List(pos(1L))

    when(mocks.houseClient.fetchRecentVotes).thenReturn(IO.pure(List(listItem)))
    when(mocks.houseClient.fetchMembersVotePositions(eqTo(119), eqTo(1), eqTo(42)))
      .thenReturn(IO.pure(membersDto))
    when(mocks.houseConverter.convert(any[repcheck.shared.models.congress.dto.vote.VoteMembersDTO], any[LogContext]))
      .thenReturn(IO.pure((vote, ps)))
    when(mocks.senateClient.fetchVoteIndex(eqTo(119), eqTo(1)))
      .thenReturn(IO.pure(List.empty[SenateVoteIndexEntry]))

    when(mocks.findStoredVote.apply(anyString())).thenReturn(IO.pure(Option.empty[VoteDO]))
    when(mocks.changeDetector.detect(any[VoteDO], any[List[VotePositionDO]], any[UUID]))
      .thenReturn(IO.pure(VoteChangeReport.New))
    when(mocks.persister.persistNew(any[VoteDO], any[List[VotePositionDO]]))
      .thenReturn(IO.pure(vote.copy(voteId = 42L)))

    val results = mocks.build.streamAll("run-1").compile.toList.unsafeRunSync()

    val _ = results.length shouldBe 1
    results.headOption.getOrElse(fail("expected one result")) match {
      case ProcessingResult.Succeeded(id, emitted) =>
        val _ = id shouldBe "119-House-1-42"
        emitted shouldBe true
      case other => fail(s"expected Succeeded, got $other")
    }
  }

  it should "process a single Senate vote end-to-end: fetch index → fetch vote → resolve LIS → convert → process" in {
    val mocks = mkMocks()

    val entry = SenateVoteIndexEntry(voteNumber = 17, voteDate = "Jan 25", question = "On Passage", result = "Passed")
    val senDto = repcheck.shared.models.congress.dto.vote.SenateVoteXmlDTO(
      congress = 119,
      session = 1,
      voteNumber = 17,
      question = "On Passage of the Bill",
      voteDate = "January 25, 2025, 11:30 AM",
      result = "Bill Passed",
      members = List.empty,
    )
    val vote = voteDO(naturalKey = "119-Senate-1-17", chamber = Chamber.Senate, billId = None)

    when(mocks.houseClient.fetchRecentVotes).thenReturn(IO.pure(List.empty))
    when(mocks.senateClient.fetchVoteIndex(eqTo(119), eqTo(1))).thenReturn(IO.pure(List(entry)))
    when(mocks.senateClient.fetchVote(eqTo(119), eqTo(1), eqTo(17))).thenReturn(IO.pure(senDto))
    when(mocks.lisResolver.resolve(eqTo(senDto))).thenReturn(IO.pure(Map.empty[String, Long]))
    when(mocks.senateConverter.convert(eqTo(senDto), any[Map[String, Long]], any[LogContext]))
      .thenReturn(IO.pure((vote, List.empty[VotePositionDO])))

    when(mocks.findStoredVote.apply(anyString())).thenReturn(IO.pure(Option.empty[VoteDO]))
    when(mocks.changeDetector.detect(any[VoteDO], any[List[VotePositionDO]], any[UUID]))
      .thenReturn(IO.pure(VoteChangeReport.New))
    when(mocks.persister.persistNew(any[VoteDO], any[List[VotePositionDO]]))
      .thenReturn(IO.pure(vote.copy(voteId = 77L)))

    val results = mocks.build.streamAll("run-1").compile.toList.unsafeRunSync()

    val _ = results.length shouldBe 1
    // Senate vote is procedural by design (billId = None) — event emitted, no stance mark
    val _ = verify(mocks.stanceRepo, never()).markHasVotes(anyLong())
    verify(mocks.eventPublisher, times(1)).voteRecorded(any[VoteRecordedEvent], any[UUID])
  }

  // ------------------------------------------------------------------
  // Per-vote failure isolation — one bad vote doesn't abort the chamber stream
  // ------------------------------------------------------------------

  it should "isolate per-vote failures on the Senate side: a failing fetchVote emits Failed, other chamber unaffected" in {
    val mocks = mkMocks()
    val entry = SenateVoteIndexEntry(voteNumber = 44, voteDate = "Mar 1", question = "On Passage", result = "Passed")

    when(mocks.houseClient.fetchRecentVotes).thenReturn(IO.pure(List.empty))
    when(mocks.senateClient.fetchVoteIndex(eqTo(119), eqTo(1))).thenReturn(IO.pure(List(entry)))
    when(mocks.senateClient.fetchVote(eqTo(119), eqTo(1), eqTo(44)))
      .thenReturn(IO.raiseError(new RuntimeException("senate xml 500")))

    val results = mocks.build.streamAll("run-1").compile.toList.unsafeRunSync()

    val failures = results.collect { case f: ProcessingResult.Failed => f }
    val _        = failures.length shouldBe 1
    val f        = failures.headOption.getOrElse(fail("expected one failure"))
    val _        = f.entityId shouldBe "119-Senate-1-44"
    f.reason should include("senate xml 500")
  }

  it should "surface an event-publish failure as a per-vote ProcessingResult.Failed (wrapped in VoteProcessingFailed)" in {
    val mocks = mkMocks()
    // Vote with updateDate=None so we exercise the voteDate fallback on line 276
    val vote = voteDO(updateDate = None)
    val ps   = List(pos(1L))

    when(mocks.findStoredVote.apply(anyString())).thenReturn(IO.pure(Option.empty[VoteDO]))
    when(mocks.changeDetector.detect(any[VoteDO], any[List[VotePositionDO]], any[UUID]))
      .thenReturn(IO.pure(VoteChangeReport.New))
    when(mocks.persister.persistNew(any[VoteDO], any[List[VotePositionDO]]))
      .thenReturn(IO.pure(vote.copy(voteId = 42L)))
    when(mocks.eventPublisher.voteRecorded(any[VoteRecordedEvent], any[UUID]))
      .thenReturn(IO.raiseError(new RuntimeException("pubsub topic not found")))

    val outcome = mocks.build
      .processVote(vote, ps, billNaturalKey = Some("119-HR-1234"), correlationId, LogContext("r", "s"))
      .attempt
      .unsafeRunSync()

    outcome match {
      case Left(e: repcheck.ingestion.votes.errors.VoteProcessingFailed) =>
        val _ = e.getMessage should include("event publish failed")
        e.getMessage should include(vote.naturalKey)
      case other => fail(s"expected Left(VoteProcessingFailed), got $other")
    }
  }

  it should "isolate per-vote failures: a failing converter on one House vote emits Failed, other chamber unaffected" in {
    val mocks = mkMocks()

    val listItem = repcheck.shared.models.congress.dto.vote.VoteListItemDTO(
      congress = 119,
      chamber = "House",
      rollCallNumber = 99,
      sessionNumber = Some(1),
      startDate = None,
      updateDate = None,
      result = None,
      voteType = None,
      legislationNumber = None,
      legislationType = None,
      legislationUrl = None,
      url = None,
      identifier = None,
      sourceDataUrl = None,
    )
    when(mocks.houseClient.fetchRecentVotes).thenReturn(IO.pure(List(listItem)))
    when(mocks.houseClient.fetchMembersVotePositions(eqTo(119), eqTo(1), eqTo(99)))
      .thenReturn(IO.raiseError(new RuntimeException("detail fetch 404")))
    when(mocks.senateClient.fetchVoteIndex(eqTo(119), eqTo(1)))
      .thenReturn(IO.pure(List.empty[SenateVoteIndexEntry]))

    val results = mocks.build.streamAll("run-1").compile.toList.unsafeRunSync()

    val failures = results.collect { case f: ProcessingResult.Failed => f }
    val _        = failures.length shouldBe 1
    val f        = failures.headOption.getOrElse(fail("expected one failure"))
    val _        = f.entityId shouldBe "119-House-1-99"
    f.reason should include("detail fetch 404")
  }

}
