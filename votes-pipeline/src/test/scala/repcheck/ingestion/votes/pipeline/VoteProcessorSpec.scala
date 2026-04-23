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
import repcheck.ingestion.bills.common.persistence.BillRepository
import repcheck.ingestion.common.events.IngestionEventPublisher
import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}
import repcheck.ingestion.common.placeholders.{EntityRepository, PlaceholderCreator}
import repcheck.ingestion.votes.api.HouseVotesApiClient
import repcheck.ingestion.votes.config.{HouseVotesConfig, SenateVoteXmlConfig}
import repcheck.ingestion.votes.lis.LisResolver
import repcheck.ingestion.votes.repo.StanceMaterializationStatusRepository
import repcheck.ingestion.votes.xml.{SenateVoteIndexEntry, SenateVoteXmlClient}
import repcheck.members.common.persistence.MemberRepository
import repcheck.pipeline.models.events.VoteRecordedEvent
import repcheck.pipeline.models.metadata.ProcessingResult
import repcheck.shared.models.congress.common.{BillType, Chamber, Party, UsState}
import repcheck.shared.models.congress.dos.bill.BillDO
import repcheck.shared.models.congress.dos.member.MemberDO
import repcheck.shared.models.congress.dos.results.VoteConversionResult
import repcheck.shared.models.congress.dos.vote.{VoteDO, VotePositionDO}
import repcheck.shared.models.congress.vote.{VoteCast, VoteMethod, VoteType}
import repcheck.shared.models.placeholder.HasPlaceholder

/**
 * Unit spec for [[VoteProcessor]]. All collaborators are mocked via MockitoScala; the spec exercises the processor's
 * orchestration without touching real HTTP or DB infrastructure. Primary focus: `processVote` (the common tail — change
 * detection → persist → event emission, trusting the converter's `billId` output) and `streamAll` chamber-level failure
 * isolation.
 *
 * Bill resolution is no longer done in `processVote` — the converters invoke the `billLookup` callback (supplied by
 * [[VoteProcessor.billLookup]]) inline during `dto.toDO(billLookup)`, so every `VoteConversionResult.vote.billId` is
 * already populated by the time `processVote` runs. The two `*Resolution` tests below pin the `billLookup` function
 * directly rather than by exercising `processVote`.
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

  private def houseConfig(p: Int = 1)  = HouseVotesConfig(congress = 119, session = 1, parallelism = p)
  private def senateConfig(p: Int = 1) = SenateVoteXmlConfig(parallelism = p)

  // Hand-rolled StubPlaceholderCreator — Mockito struggles with Scala 3's `using` arg list on ensureExists.
  final private class StubPlaceholderCreator extends PlaceholderCreator[IO] {

    def ensureExists[T <: Product](
      naturalKey: String,
      repository: EntityRepository[IO, T],
    )(using HasPlaceholder[T]): IO[Unit] = IO.unit

  }

  private def voteDO(
    voteId: Long = 0L,
    naturalKey: String = "119-House-1-42",
    chamber: Chamber = Chamber.House,
    billId: Option[Long] = None,
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

  /** All collaborators bundled so each test doesn't re-thread the giant arg list. */
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
    memberRepo: MemberRepository,
    billRepo: BillRepository[ConnectionIO],
    memberEntityRepo: EntityRepository[IO, MemberDO],
    billEntityRepo: EntityRepository[IO, BillDO],
    findStoredVote: String => IO[Option[VoteDO]],
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
      memberRepo = memberRepo,
      billRepo = billRepo,
      memberEntityRepo = memberEntityRepo,
      billEntityRepo = billEntityRepo,
      placeholderCreator = new StubPlaceholderCreator,
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
      memberRepo = mock[MemberRepository],
      billRepo = mock[BillRepository[ConnectionIO]],
      memberEntityRepo = mock[EntityRepository[IO, MemberDO]],
      billEntityRepo = mock[EntityRepository[IO, BillDO]],
      findStoredVote = mock[String => IO[Option[VoteDO]]],
    )
    // Sensible defaults — tests override as needed.
    when(mocks.stanceRepo.markHasVotes(anyLong())).thenReturn(connection.pure(()))
    when(mocks.eventPublisher.voteRecorded(any[VoteRecordedEvent], any[UUID])).thenReturn(IO.pure("msg-id"))
    mocks
  }

  // ------------------------------------------------------------------
  // processVote — decision matrix (billId already populated by converter)
  // ------------------------------------------------------------------

  "processVote" should "persist New + emit event(isUpdate=false) for procedural votes and return Succeeded" in {
    val mocks = mkMocks()
    // Procedural: converter produced billId=None (no legislation reference).
    val vote       = voteDO(billId = None)
    val conversion = VoteConversionResult(vote, billNaturalKey = None, positions = List.empty)
    val buildPositions: Long => List[VotePositionDO] = _ => List.empty

    when(mocks.findStoredVote.apply(anyString())).thenReturn(IO.pure(Option.empty[VoteDO]))
    when(mocks.changeDetector.detect(any[VoteDO], any[List[VotePositionDO]], any[UUID]))
      .thenReturn(IO.pure(VoteChangeReport.New))
    when(mocks.persister.persistNew(any[VoteDO], any()))
      .thenReturn(IO.pure(vote.copy(voteId = 42L)))

    val result = mocks.build
      .processVote(conversion, buildPositions, dtoBillNaturalKey = None, correlationId, LogContext("r", "s"))
      .unsafeRunSync()

    val _ = result shouldBe ProcessingResult.Succeeded(vote.naturalKey, eventEmitted = true)
    // Stance mark never fires for procedural votes (mechanical — schema has no row shape for them).
    val _ = verify(mocks.stanceRepo, never()).markHasVotes(anyLong())
    // Event still fires — downstream consumers decide what to do with billNaturalKey=None.
    val captor = ArgumentCaptor.forClass(classOf[VoteRecordedEvent])
    val _      = verify(mocks.eventPublisher, times(1)).voteRecorded(captor.capture(), any[UUID])
    val _      = captor.getValue.billNaturalKey shouldBe None
    captor.getValue.isUpdate shouldBe false
  }

  it should "persist Updated(positionsChanged=true) + emit event(isUpdate=true) + mark stance when bill-linked" in {
    val mocks = mkMocks()
    // Converter output already carries the resolved billId; no processor-level override.
    val vote       = voteDO(billId = Some(200L))
    val stored     = vote.copy(voteId = 77L)
    val conversion = VoteConversionResult(vote, billNaturalKey = Some("119-HR-9999"), positions = List.empty)
    val buildPositions: Long => List[VotePositionDO] = _ => List(pos(1L))

    when(mocks.findStoredVote.apply(anyString())).thenReturn(IO.pure(Some(stored)))
    val diff = DiffResult.ValueResult.Both("a", "b", isSame = false, isIgnored = false)
    when(mocks.changeDetector.detect(any[VoteDO], any[List[VotePositionDO]], any[UUID]))
      .thenReturn(IO.pure(VoteChangeReport.Updated(positionsChanged = true, diff = diff)))
    when(mocks.persister.persistUpdate(any[VoteDO], anyLong(), any()))
      .thenReturn(IO.pure(stored.copy(voteId = 42L)))

    val result = mocks.build
      .processVote(
        conversion,
        buildPositions,
        dtoBillNaturalKey = None,
        correlationId,
        LogContext("r", "s"),
      )
      .unsafeRunSync()

    val _ = result shouldBe ProcessingResult.Succeeded(vote.naturalKey, eventEmitted = true)
    // persistUpdate was called with the stored voteId (77L)
    val _ = verify(mocks.persister, times(1))
      .persistUpdate(any[VoteDO], eqTo(77L), any())
    // Stance mark fires because the persisted vote has billId = Some(200L)
    val _      = verify(mocks.stanceRepo, times(1)).markHasVotes(200L)
    val captor = ArgumentCaptor.forClass(classOf[VoteRecordedEvent])
    val _      = verify(mocks.eventPublisher, times(1)).voteRecorded(captor.capture(), any[UUID])
    val _      = captor.getValue.isUpdate shouldBe true
    captor.getValue.billNaturalKey shouldBe Some("119-HR-9999")
  }

  it should "persist Updated(positionsChanged=false) via persistMetadataOnlyUpdate and SKIP stance+event" in {
    val mocks      = mkMocks()
    val vote       = voteDO(billId = Some(300L))
    val stored     = vote.copy(voteId = 88L)
    val conversion = VoteConversionResult(vote, billNaturalKey = None, positions = List.empty)
    val buildPositions: Long => List[VotePositionDO] = _ => List.empty

    when(mocks.findStoredVote.apply(anyString())).thenReturn(IO.pure(Some(stored)))
    val diff = DiffResult.ValueResult.Both("x", "x", isSame = true, isIgnored = false)
    when(mocks.changeDetector.detect(any[VoteDO], any[List[VotePositionDO]], any[UUID]))
      .thenReturn(IO.pure(VoteChangeReport.Updated(positionsChanged = false, diff = diff)))
    when(mocks.persister.persistMetadataOnlyUpdate(any[VoteDO], anyLong()))
      .thenReturn(IO.pure(stored.copy(voteId = 42L)))

    val result = mocks.build
      .processVote(conversion, buildPositions, dtoBillNaturalKey = None, correlationId, LogContext("r", "s"))
      .unsafeRunSync()

    val _ = result shouldBe ProcessingResult.Succeeded(vote.naturalKey, eventEmitted = false)
    val _ = verify(mocks.persister, times(1)).persistMetadataOnlyUpdate(any[VoteDO], eqTo(88L))
    val _ = verify(mocks.persister, never()).persistNew(any[VoteDO], any())
    val _ =
      verify(mocks.persister, never()).persistUpdate(any[VoteDO], anyLong(), any())
    val _ = verify(mocks.stanceRepo, never()).markHasVotes(anyLong())
    verify(mocks.eventPublisher, never()).voteRecorded(any[VoteRecordedEvent], any[UUID])
  }

  it should "no-op on Unchanged and return Skipped(reason=unchanged)" in {
    val mocks      = mkMocks()
    val vote       = voteDO()
    val conversion = VoteConversionResult(vote, billNaturalKey = None, positions = List.empty)
    val buildPositions: Long => List[VotePositionDO] = _ => List.empty

    when(mocks.findStoredVote.apply(anyString())).thenReturn(IO.pure(Some(vote.copy(voteId = 99L))))
    when(mocks.changeDetector.detect(any[VoteDO], any[List[VotePositionDO]], any[UUID]))
      .thenReturn(IO.pure(VoteChangeReport.Unchanged))

    val result = mocks.build
      .processVote(conversion, buildPositions, dtoBillNaturalKey = None, correlationId, LogContext("r", "s"))
      .unsafeRunSync()

    val _ = result shouldBe ProcessingResult.Skipped(vote.naturalKey, "unchanged")
    val _ = verify(mocks.persister, never()).persistNew(any[VoteDO], any())
    val _ = verify(mocks.persister, never()).persistMetadataOnlyUpdate(any[VoteDO], anyLong())
    val _ = verify(mocks.stanceRepo, never()).markHasVotes(anyLong())
    verify(mocks.eventPublisher, never()).voteRecorded(any[VoteRecordedEvent], any[UUID])
  }

  // ------------------------------------------------------------------
  // Chamber-level failure isolation
  // ------------------------------------------------------------------

  "streamAll" should "emit house-chamber Failed when fetchRecentVotes raises; senate stream still runs" in {
    val mocks = mkMocks()
    when(mocks.houseClient.fetchRecentVotes).thenReturn(IO.raiseError(new RuntimeException("house 401")))
    when(mocks.senateClient.fetchVoteIndex(eqTo(119), eqTo(1)))
      .thenReturn(IO.pure(List.empty[SenateVoteIndexEntry]))

    val results = mocks.build.streamAll("run-1").compile.toList.unsafeRunSync()

    val houseFailures = results.collect {
      case f @ ProcessingResult.Failed(id, _, _) if id == "house-chamber" => f
    }
    val _ = houseFailures.length shouldBe 1
    houseFailures.headOption.foreach(f => f.reason should include("house 401"))
  }

  it should "emit senate-chamber Failed when fetchVoteIndex raises" in {
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
  // Event publish failure surfaces as VoteProcessingFailed
  // ------------------------------------------------------------------

  it should "surface an event-publish failure as VoteProcessingFailed (caught by per-vote handleErrorWith)" in {
    val mocks      = mkMocks()
    val vote       = voteDO(billId = None, updateDate = None)
    val conversion = VoteConversionResult(vote, billNaturalKey = None, positions = List.empty)
    val buildPositions: Long => List[VotePositionDO] = _ => List.empty

    when(mocks.findStoredVote.apply(anyString())).thenReturn(IO.pure(Option.empty[VoteDO]))
    when(mocks.changeDetector.detect(any[VoteDO], any[List[VotePositionDO]], any[UUID]))
      .thenReturn(IO.pure(VoteChangeReport.New))
    when(mocks.persister.persistNew(any[VoteDO], any()))
      .thenReturn(IO.pure(vote.copy(voteId = 42L)))
    when(mocks.eventPublisher.voteRecorded(any[VoteRecordedEvent], any[UUID]))
      .thenReturn(IO.raiseError(new RuntimeException("pubsub topic not found")))

    val outcome = mocks.build
      .processVote(conversion, buildPositions, dtoBillNaturalKey = None, correlationId, LogContext("r", "s"))
      .attempt
      .unsafeRunSync()

    outcome match {
      case Left(e: repcheck.ingestion.votes.errors.VoteProcessingFailed) =>
        val _ = e.getMessage should include("event publish failed")
        e.getMessage should include(vote.naturalKey)
      case other => fail(s"expected Left(VoteProcessingFailed), got $other")
    }
  }

  // ------------------------------------------------------------------
  // End-to-end House + Senate via streamAll (covers processHouseVote + processSenateVote + buildPositions)
  // ------------------------------------------------------------------

  it should "process one House vote end-to-end via streamAll: fetch list → fetch members → convert → resolve members → persist + emit" in {
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
    // Converter (mocked) is expected to populate billId via its injected billLookup. We stub its output
    // directly with the resolved id; no processor-level override is exercised.
    val vote = voteDO(naturalKey = "119-House-1-42", billId = Some(900L))
    val unresolved = repcheck.shared.models.congress.dos.results.UnresolvedVotePosition(
      memberSource = Left("A000055"),
      voteCast = Some(VoteCast.Yea),
      partyAtVote = Some(Party.Democrat),
      stateAtVote = Some(UsState.NewYork),
    )

    when(mocks.houseClient.fetchRecentVotes).thenReturn(IO.pure(List(listItem)))
    when(mocks.houseClient.fetchMembersVotePositions(eqTo(119), eqTo(1), eqTo(42)))
      .thenReturn(IO.pure(membersDto))
    when(
      mocks.houseConverter.convert(
        any[repcheck.shared.models.congress.dto.vote.VoteMembersDTO],
        any(),
        any[LogContext],
      )
    ).thenReturn(
      IO.pure(VoteConversionResult(vote, billNaturalKey = Some("119-HR-1234"), positions = List(unresolved)))
    )

    // Member resolution: placeholder creator is a no-op stub; memberRepo returns Some(MemberDO) with memberId=7
    val resolvedMember = mock[repcheck.shared.models.congress.dos.member.MemberDO]
    when(resolvedMember.memberId).thenReturn(7L)
    when(mocks.memberRepo.findByBioguideId(eqTo("A000055")))
      .thenReturn(connection.pure(Some(resolvedMember)))

    when(mocks.senateClient.fetchVoteIndex(eqTo(119), eqTo(1)))
      .thenReturn(IO.pure(List.empty[SenateVoteIndexEntry]))

    when(mocks.findStoredVote.apply(anyString())).thenReturn(IO.pure(Option.empty[VoteDO]))
    when(mocks.changeDetector.detect(any[VoteDO], any[List[VotePositionDO]], any[UUID]))
      .thenReturn(IO.pure(VoteChangeReport.New))
    when(mocks.persister.persistNew(any[VoteDO], any()))
      .thenReturn(IO.pure(vote.copy(voteId = 42L)))

    val results = mocks.build.streamAll("run-1").compile.toList.unsafeRunSync()

    val _ = results.length shouldBe 1
    val _ = verify(mocks.stanceRepo, times(1)).markHasVotes(900L)
    results.headOption.getOrElse(fail("expected one result")) match {
      case ProcessingResult.Succeeded(id, emitted) =>
        val _ = id shouldBe "119-House-1-42"
        emitted shouldBe true
      case other => fail(s"expected Succeeded, got $other")
    }
  }

  it should "process one Senate vote end-to-end via streamAll: fetch index → fetch XML → LIS resolve → convert → persist + emit" in {
    val mocks = mkMocks()

    val entry = SenateVoteIndexEntry(voteNumber = 17, voteDate = "Jan 25", question = "On Passage", result = "Passed")
    val senDoc = repcheck.shared.models.congress.dto.vote.SenateVoteDocumentDTO(
      documentCongress = 119,
      documentType = "S.",
      documentNumber = "1071",
      documentName = "S. 1071",
      documentTitle = "Title",
      documentShortTitle = None,
    )
    val senDto = repcheck.shared.models.congress.dto.vote.SenateVoteXmlDTO(
      congress = 119,
      session = 1,
      voteNumber = 17,
      question = "On Passage of the Bill",
      voteDate = "January 25, 2025, 11:30 AM",
      result = "Bill Passed",
      document = senDoc,
      members = List.empty,
    )
    // Converter (mocked) populates billId=Some(500) directly — mimicking its real billLookup-driven behavior.
    val vote = voteDO(naturalKey = "119-Senate-1-17", chamber = Chamber.Senate, billId = Some(500L))
    val unresolved = repcheck.shared.models.congress.dos.results.UnresolvedVotePosition(
      memberSource = Right("S428"),
      voteCast = Some(VoteCast.Yea),
      partyAtVote = Some(Party.Democrat),
      stateAtVote = Some(UsState.NewYork),
    )

    when(mocks.houseClient.fetchRecentVotes).thenReturn(IO.pure(List.empty))
    when(mocks.senateClient.fetchVoteIndex(eqTo(119), eqTo(1))).thenReturn(IO.pure(List(entry)))
    when(mocks.senateClient.fetchVote(eqTo(119), eqTo(1), eqTo(17))).thenReturn(IO.pure(senDto))
    when(mocks.lisResolver.resolve(eqTo(senDto))).thenReturn(IO.pure(Map("S428" -> 99L)))
    when(
      mocks.senateConverter.convert(
        eqTo(senDto),
        any(),
        any[LogContext],
      )
    ).thenReturn(
      IO.pure(VoteConversionResult(vote, billNaturalKey = Some("119-S-1071"), positions = List(unresolved)))
    )

    when(mocks.findStoredVote.apply(anyString())).thenReturn(IO.pure(Option.empty[VoteDO]))
    when(mocks.changeDetector.detect(any[VoteDO], any[List[VotePositionDO]], any[UUID]))
      .thenReturn(IO.pure(VoteChangeReport.New))
    when(mocks.persister.persistNew(any[VoteDO], any()))
      .thenReturn(IO.pure(vote.copy(voteId = 77L)))

    val results = mocks.build.streamAll("run-1").compile.toList.unsafeRunSync()

    val _ = results.length shouldBe 1
    // Stance mark fires because the persisted vote has billId = Some(500L)
    val _ = verify(mocks.stanceRepo, times(1)).markHasVotes(500L)
    verify(mocks.eventPublisher, times(1)).voteRecorded(any[VoteRecordedEvent], any[UUID])
  }

  // ------------------------------------------------------------------
  // billLookup callback — Mirror of the prior resolveOptionalBillId tests, now pinning the callback directly
  // ------------------------------------------------------------------

  "billLookup" should "resolve to Some(billId) after placeholder + findByBillId succeed" in {
    val mocks        = mkMocks()
    val resolvedBill = mock[repcheck.shared.models.congress.dos.bill.BillDO]
    when(resolvedBill.billId).thenReturn(404L)
    when(mocks.billRepo.findByBillId(eqTo("119-HR-1234"))).thenReturn(connection.pure(Some(resolvedBill)))

    val lookup = mocks.build.billLookup(LogContext("r", "s"))
    lookup("119-HR-1234").unsafeRunSync() shouldBe Some(404L)
  }

  it should "raise BillResolutionFailed when findByBillId returns None after ensureExists" in {
    val mocks = mkMocks()
    when(mocks.billRepo.findByBillId(eqTo("119-HR-1234"))).thenReturn(connection.pure(Option.empty))

    val lookup  = mocks.build.billLookup(LogContext("r", "s"))
    val outcome = lookup("119-HR-1234").attempt.unsafeRunSync()

    outcome match {
      case Left(e: repcheck.ingestion.votes.errors.BillResolutionFailed) =>
        e.billNaturalKey shouldBe "119-HR-1234"
      case other => fail(s"expected Left(BillResolutionFailed), got $other")
    }
  }

  "resolveHouseMembers" should "short-circuit on empty position list" in {
    val mocks = mkMocks()
    mocks.build.resolveHouseMembers(List.empty, LogContext("r", "s")).unsafeRunSync() shouldBe Map.empty[String, Long]
  }

  it should "raise MemberResolutionFailed when a bioguide lookup returns None after ensureExists" in {
    val mocks = mkMocks()
    when(mocks.memberRepo.findByBioguideId(eqTo("B000999"))).thenReturn(connection.pure(Option.empty))

    val unresolved = repcheck.shared.models.congress.dos.results.UnresolvedVotePosition(
      memberSource = Left("B000999"),
      voteCast = Some(VoteCast.Yea),
      partyAtVote = Some(Party.Democrat),
      stateAtVote = Some(UsState.NewYork),
    )

    val outcome = mocks.build
      .resolveHouseMembers(List(unresolved), LogContext("r", "s"))
      .attempt
      .unsafeRunSync()

    outcome match {
      case Left(e: repcheck.ingestion.votes.errors.MemberResolutionFailed) =>
        e.bioguideId shouldBe "B000999"
      case other => fail(s"expected Left(MemberResolutionFailed), got $other")
    }
  }

  // ------------------------------------------------------------------
  // Position builders — pure helpers
  // ------------------------------------------------------------------

  "buildHousePositions" should "materialize House-arm VotePositionDOs with the given voteId + resolved member_ids" in {
    val mocks = mkMocks()
    val unresolved = List(
      repcheck.shared.models.congress.dos.results.UnresolvedVotePosition(
        memberSource = Left("A000055"),
        voteCast = Some(VoteCast.Yea),
        partyAtVote = Some(Party.Democrat),
        stateAtVote = Some(UsState.NewYork),
      ),
      repcheck.shared.models.congress.dos.results.UnresolvedVotePosition(
        memberSource = Right("S428"), // Senate entry — should be dropped by House builder
        voteCast = Some(VoteCast.Nay),
        partyAtVote = Some(Party.Democrat),
        stateAtVote = Some(UsState.Maryland),
      ),
    )
    val result = mocks.build.buildHousePositions(42L, unresolved, Map("A000055" -> 7L))

    val _ = result.length shouldBe 1
    val p = result.headOption.getOrElse(fail("expected one position"))
    val _ = p.voteId shouldBe 42L
    val _ = p.memberId shouldBe Some(7L)
    p.lisMemberId shouldBe None
  }

  "buildSenatePositions" should "materialize Senate-arm VotePositionDOs with the given voteId + resolved lis_members.id" in {
    val mocks = mkMocks()
    val unresolved = List(
      repcheck.shared.models.congress.dos.results.UnresolvedVotePosition(
        memberSource = Right("S428"),
        voteCast = Some(VoteCast.Yea),
        partyAtVote = Some(Party.Democrat),
        stateAtVote = Some(UsState.NewYork),
      ),
      repcheck.shared.models.congress.dos.results.UnresolvedVotePosition(
        memberSource = Left("A000055"), // House entry — dropped by Senate builder
        voteCast = Some(VoteCast.Nay),
        partyAtVote = Some(Party.Democrat),
        stateAtVote = Some(UsState.Maryland),
      ),
    )
    val result = mocks.build.buildSenatePositions(88L, unresolved, Map("S428" -> 99L))

    val _ = result.length shouldBe 1
    val p = result.headOption.getOrElse(fail("expected one position"))
    val _ = p.voteId shouldBe 88L
    val _ = p.lisMemberId shouldBe Some(99L)
    p.memberId shouldBe None
  }

  "buildHouseNaturalKey" should "use the DTO's sessionNumber when present" in {
    val mocks = mkMocks()
    val listItem = repcheck.shared.models.congress.dto.vote.VoteListItemDTO(
      congress = 119,
      chamber = "House",
      rollCallNumber = 5,
      sessionNumber = Some(2),
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
    mocks.build.buildHouseNaturalKey(listItem) shouldBe "119-House-2-5"
  }

  // ------------------------------------------------------------------
  // Per-vote failure isolation (as distinct from chamber-level failure isolation)
  // ------------------------------------------------------------------

  it should "isolate House per-vote failures: fetchMembersVotePositions raises, chamber stream continues" in {
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
      .thenReturn(IO.raiseError(new RuntimeException("fetch detail 404")))
    when(mocks.senateClient.fetchVoteIndex(eqTo(119), eqTo(1)))
      .thenReturn(IO.pure(List.empty[SenateVoteIndexEntry]))

    val results = mocks.build.streamAll("run-1").compile.toList.unsafeRunSync()

    val perVoteFailures = results.collect { case f: ProcessingResult.Failed => f }
    val _               = perVoteFailures.length shouldBe 1
    val f               = perVoteFailures.headOption.getOrElse(fail("expected one per-vote failure"))
    val _               = f.entityId shouldBe "119-House-1-99"
    f.reason should include("fetch detail 404")
  }

  it should "isolate Senate per-vote failures: fetchVote raises, chamber stream continues" in {
    val mocks = mkMocks()
    val entry = SenateVoteIndexEntry(voteNumber = 44, voteDate = "Mar 1", question = "On Passage", result = "Passed")

    when(mocks.houseClient.fetchRecentVotes).thenReturn(IO.pure(List.empty))
    when(mocks.senateClient.fetchVoteIndex(eqTo(119), eqTo(1))).thenReturn(IO.pure(List(entry)))
    when(mocks.senateClient.fetchVote(eqTo(119), eqTo(1), eqTo(44)))
      .thenReturn(IO.raiseError(new RuntimeException("senate xml 500")))

    val results = mocks.build.streamAll("run-1").compile.toList.unsafeRunSync()

    val perVoteFailures = results.collect { case f: ProcessingResult.Failed => f }
    val _               = perVoteFailures.length shouldBe 1
    val f               = perVoteFailures.headOption.getOrElse(fail("expected one per-vote failure"))
    val _               = f.entityId shouldBe "119-Senate-1-44"
    f.reason should include("senate xml 500")
  }

  "buildSenateBillNaturalKey" should "always return None (senate paths go through the converter's classifyDocument)" in {
    val mocks = mkMocks()
    val doc = repcheck.shared.models.congress.dto.vote.SenateVoteDocumentDTO(
      documentCongress = 119,
      documentType = "S.",
      documentNumber = "1",
      documentName = "S. 1",
      documentTitle = "Test",
      documentShortTitle = None,
    )
    val dto = repcheck.shared.models.congress.dto.vote.SenateVoteXmlDTO(
      congress = 119,
      session = 1,
      voteNumber = 1,
      question = "Q",
      voteDate = "2025-01-25T00:00:00",
      result = "Passed",
      document = doc,
      members = List.empty,
    )
    mocks.build.buildSenateBillNaturalKey(dto) shouldBe None
  }

}
