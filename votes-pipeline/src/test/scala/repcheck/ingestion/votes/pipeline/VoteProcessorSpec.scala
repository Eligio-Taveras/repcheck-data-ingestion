package repcheck.ingestion.votes.pipeline

import java.time.{Instant, LocalDate}
import java.util.UUID

import cats.effect.IO
import cats.effect.unsafe.implicits.global

import difflicious.DiffResult
import org.mockito.ArgumentMatchers.{any, anyLong, anyString, eq => eqTo}
import org.mockito.Mockito.{never, times, verify, when}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar
import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}
import repcheck.ingestion.votes.api.HouseVotesApiClient
import repcheck.ingestion.votes.config.{HouseVotesConfig, SenateVoteXmlConfig}
import repcheck.ingestion.votes.lis.LisResolver
import repcheck.ingestion.votes.xml.{SenateVoteIndexEntry, SenateVoteXmlClient}
import repcheck.pipeline.models.metadata.ProcessingResult
import repcheck.shared.models.congress.common.{BillType, Chamber, Party, UsState}
import repcheck.shared.models.congress.dos.results.VoteConversionResult
import repcheck.shared.models.congress.dos.vote.{VoteDO, VotePositionDO}
import repcheck.shared.models.congress.vote.{VoteCast, VoteMethod, VoteType}

/**
 * Unit spec for [[VoteProcessor]]. The processor is a thin orchestrator over five collaborators: [[BillLookup]],
 * [[MemberLookup]], [[VoteEventEmitter]], [[VotePositionBuilders]] (via pure callers), and [[VoteNaturalKeys]] (via
 * pure callers) — each tested in its own spec. This file focuses on the decision matrix (`processVote` +
 * `dispatchOnReport`) and chamber-level stream orchestration (`streamAll` merge + failure isolation). Collaborators
 * that have their own specs are mocked here; behavioral assertions that used to live inside `VoteProcessor` moved to
 * `BillLookupSpec`, `MemberLookupSpec`, `VoteEventEmitterSpec`, `VotePositionBuildersSpec`, and `VoteNaturalKeysSpec`.
 */
class VoteProcessorSpec extends AnyFlatSpec with Matchers with MockitoSugar {

  private val correlationId: UUID = UUID.fromString("33333333-4444-5555-6666-777777777777")

  private def houseConfig(p: Int = 1)  = HouseVotesConfig(congress = 119, session = 1, parallelism = p)
  private def senateConfig(p: Int = 1) = SenateVoteXmlConfig(parallelism = p)

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
    billLookup: BillLookup[IO],
    memberLookup: MemberLookup[IO],
    eventEmitter: VoteEventEmitter[IO],
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
      billLookup = billLookup,
      memberLookup = memberLookup,
      eventEmitter = eventEmitter,
      findStoredVote = findStoredVote,
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
      billLookup = mock[BillLookup[IO]],
      memberLookup = mock[MemberLookup[IO]],
      eventEmitter = mock[VoteEventEmitter[IO]],
      findStoredVote = mock[String => IO[Option[VoteDO]]],
    )
    // Sensible defaults — tests override as needed. The emitter is a no-op by default so `emitSuccess` calls don't
    // affect the persistence branch's return value; individual tests verify the emitter was or wasn't invoked.
    when(
      mocks.eventEmitter.emitSuccess(
        any[VoteDO],
        any[Option[String]],
        any[Boolean],
        any[UUID],
        any[LogContext],
      )
    ).thenReturn(IO.unit)
    when(mocks.billLookup.forContext(any[LogContext])).thenReturn((_: String) => IO.pure(Option.empty[Long]))
    when(mocks.memberLookup.resolveAll(any(), any[LogContext])).thenReturn(IO.pure(Map.empty[String, Long]))
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
    // emitSuccess fired with the persisted vote + billNaturalKey=None + isUpdate=false
    verify(mocks.eventEmitter, times(1))
      .emitSuccess(any[VoteDO], eqTo(Option.empty[String]), eqTo(false), any[UUID], any[LogContext])
  }

  it should "persist Updated(positionsChanged=true) + emit event(isUpdate=true) when bill-linked" in {
    val mocks      = mkMocks()
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
      .processVote(conversion, buildPositions, dtoBillNaturalKey = None, correlationId, LogContext("r", "s"))
      .unsafeRunSync()

    val _ = result shouldBe ProcessingResult.Succeeded(vote.naturalKey, eventEmitted = true)
    // persistUpdate was called with the stored voteId (77L)
    val _ = verify(mocks.persister, times(1))
      .persistUpdate(any[VoteDO], eqTo(77L), any())
    // emitSuccess fired with billNaturalKey = Some("119-HR-9999") + isUpdate=true
    verify(mocks.eventEmitter, times(1))
      .emitSuccess(any[VoteDO], eqTo(Some("119-HR-9999")), eqTo(true), any[UUID], any[LogContext])
  }

  it should "persist Updated(positionsChanged=false) via persistMetadataOnlyUpdate and SKIP emitSuccess" in {
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
    val _ = verify(mocks.persister, never()).persistUpdate(any[VoteDO], anyLong(), any())
    verify(mocks.eventEmitter, never())
      .emitSuccess(any[VoteDO], any[Option[String]], any[Boolean], any[UUID], any[LogContext])
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
    verify(mocks.eventEmitter, never())
      .emitSuccess(any[VoteDO], any[Option[String]], any[Boolean], any[UUID], any[LogContext])
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
  // End-to-end House + Senate via streamAll (covers processHouseVote + processSenateVote wiring)
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
    // Converter (mocked) populates billId via its injected billLookup. We stub its output directly with the
    // resolved id; no processor-level override is exercised.
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
    when(mocks.memberLookup.resolveAll(any(), any[LogContext])).thenReturn(IO.pure(Map("A000055" -> 7L)))

    when(mocks.senateClient.fetchVoteIndex(eqTo(119), eqTo(1)))
      .thenReturn(IO.pure(List.empty[SenateVoteIndexEntry]))

    when(mocks.findStoredVote.apply(anyString())).thenReturn(IO.pure(Option.empty[VoteDO]))
    when(mocks.changeDetector.detect(any[VoteDO], any[List[VotePositionDO]], any[UUID]))
      .thenReturn(IO.pure(VoteChangeReport.New))
    when(mocks.persister.persistNew(any[VoteDO], any()))
      .thenReturn(IO.pure(vote.copy(voteId = 42L)))

    val results = mocks.build.streamAll("run-1").compile.toList.unsafeRunSync()

    val _ = results.length shouldBe 1
    // emitSuccess fires once with the persisted vote (billId=Some(900L)) — the emitter's internal stance-mark is
    // covered in VoteEventEmitterSpec.
    val _ = verify(mocks.eventEmitter, times(1))
      .emitSuccess(any[VoteDO], any[Option[String]], any[Boolean], any[UUID], any[LogContext])
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
    verify(mocks.eventEmitter, times(1))
      .emitSuccess(any[VoteDO], any[Option[String]], any[Boolean], any[UUID], any[LogContext])
  }

  // ------------------------------------------------------------------
  // Per-vote failure isolation (distinct from chamber-level failure isolation)
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

}
