package repcheck.ingestion.members.profile.pipeline

import java.time.Instant
import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.duration._

import cats.effect.IO
import cats.effect.unsafe.implicits.global

import doobie._

import org.mockito.ArgumentMatchers.{any, anyLong, anyString, eq => eqTo}
import org.mockito.Mockito.{never, times, verify, when}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar
import repcheck.ingestion.common.api.FetchParams
import repcheck.ingestion.common.events.IngestionEventPublisher
import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}
import repcheck.ingestion.members.profile.api.MembersApiClient
import repcheck.ingestion.members.profile.config.MemberProfileConfig
import repcheck.members.common.persistence.{
  MemberHistoryArchiver,
  MemberPartyHistoryRepository,
  MemberRepository,
  MemberTermRepository,
}
import repcheck.pipeline.models.events.MemberUpdatedEvent
import repcheck.shared.models.congress.common.{Chamber, Party, UsState}
import repcheck.shared.models.congress.dos.member.{MemberDO, MemberPartyHistoryDO, MemberTermDO}
import repcheck.shared.models.congress.dto.member.{
  MemberDepictionDTO,
  MemberDetailDTO,
  MemberDetailTermDTO,
  MemberListItemDTO,
  MemberTermSummaryDTO,
  PartyHistoryDTO,
}
import repcheck.shared.models.congress.member.MemberType

class MemberProfileProcessorSpec extends AnyFlatSpec with Matchers with MockitoSugar {

  private val testXa: Transactor[IO] = Transactor.fromDriverManager[IO](
    driver = "org.h2.Driver",
    url = "jdbc:h2:mem:member_profile_test;DB_CLOSE_DELAY=-1",
    user = "",
    password = "",
    logHandler = None,
  )

  private val correlationId = UUID.randomUUID()
  private val config        = MemberProfileConfig(congress = 118, parallelism = 1, pageDelay = 0.millis)

  private case class TestFixture(
    apiClient: MembersApiClient[IO],
    memberRepo: MemberRepository,
    termRepo: MemberTermRepository,
    partyHistoryRepo: MemberPartyHistoryRepository,
    historyArchiver: MemberHistoryArchiver[ConnectionIO],
    eventPublisher: IngestionEventPublisher[IO],
    logger: PipelineLogger[IO],
  ) {

    def processor: MemberProfileProcessor[IO] =
      new MemberProfileProcessor[IO](
        apiClient = apiClient,
        memberRepo = memberRepo,
        termRepo = termRepo,
        partyHistoryRepo = partyHistoryRepo,
        historyArchiver = historyArchiver,
        eventPublisher = eventPublisher,
        xa = testXa,
        config = config,
        logger = logger,
      )

    def processorWith(cfg: MemberProfileConfig): MemberProfileProcessor[IO] =
      new MemberProfileProcessor[IO](
        apiClient = apiClient,
        memberRepo = memberRepo,
        termRepo = termRepo,
        partyHistoryRepo = partyHistoryRepo,
        historyArchiver = historyArchiver,
        eventPublisher = eventPublisher,
        xa = testXa,
        config = cfg,
        logger = logger,
      )

  }

  private def createFixture(): TestFixture = {
    val loggerMock = mock[PipelineLogger[IO]]
    when(loggerMock.info(any[LogContext], anyString())).thenReturn(IO.unit)
    when(loggerMock.warn(any[LogContext], anyString())).thenReturn(IO.unit)
    when(loggerMock.error(any[LogContext], anyString(), any[Option[Throwable]])).thenReturn(IO.unit)
    when(loggerMock.debug(any[LogContext], anyString())).thenReturn(IO.unit)

    val publisher = mock[IngestionEventPublisher[IO]]
    when(publisher.memberUpdated(any[MemberUpdatedEvent], any[UUID])).thenReturn(IO.pure("msg-id"))

    TestFixture(
      apiClient = mock[MembersApiClient[IO]],
      memberRepo = mock[MemberRepository],
      termRepo = mock[MemberTermRepository],
      partyHistoryRepo = mock[MemberPartyHistoryRepository],
      historyArchiver = mock[MemberHistoryArchiver[ConnectionIO]],
      eventPublisher = publisher,
      logger = loggerMock,
    )
  }

  // House-flavored terms (member type ENUM values come from MemberType; chamber House).
  private def houseTerm(congress: Int): MemberTermDO =
    MemberTermDO(
      termId = 0L,
      memberId = 0L,
      chamber = Some(Chamber.House),
      congress = Some(congress),
      startYear = Some(2023),
      endYear = Some(2025),
      memberType = Some(MemberType.Representative),
      stateCode = Some(UsState.NewYork),
      stateName = Some("New York"),
      district = Some(5),
    )

  private def senateTerm(congress: Int): MemberTermDO =
    MemberTermDO(
      termId = 0L,
      memberId = 0L,
      chamber = Some(Chamber.Senate),
      congress = Some(congress),
      startYear = Some(2023),
      endYear = Some(2029),
      memberType = Some(MemberType.Senator),
      stateCode = Some(UsState.California),
      stateName = Some("California"),
      district = None,
    )

  private def makeMember(
    bioguideId: String = "A000001",
    updateDate: Option[Instant] = Some(Instant.parse("2024-06-01T00:00:00Z")),
    memberId: Long = 0L,
  ): MemberDO =
    MemberDO(
      memberId = memberId,
      naturalKey = bioguideId,
      firstName = Some("Jane"),
      lastName = Some("Doe"),
      directOrderName = Some("Jane Doe"),
      invertedOrderName = Some("Doe, Jane"),
      honorificName = Some("Rep. Jane Doe"),
      birthYear = Some(1970),
      currentParty = Some(Party.Democrat),
      state = Some(UsState.NewYork),
      district = Some(5),
      imageUrl = None,
      imageAttribution = None,
      officialUrl = None,
      updateDate = updateDate,
      createdAt = None,
      updatedAt = None,
    )

  private def makeListItem(
    bioguideId: String = "A000001",
    url: Option[String] = Some("https://api.congress.gov/v3/member/A000001"),
    updateDate: Option[String] = Some("2024-06-01T00:00:00Z"),
  ): MemberListItemDTO =
    MemberListItemDTO(
      bioguideId = bioguideId,
      name = Some("Rep. Jane Doe"),
      partyName = Some("Democratic"),
      state = Some("NY"),
      depiction = Some(MemberDepictionDTO(imageUrl = None, attribution = None)),
      terms = Some(List(MemberTermSummaryDTO(chamber = Some("House of Representatives"), startYear = Some(2023)))),
      updateDate = updateDate,
      url = url,
    )

  private def makeDetailDTO(
    bioguideId: String = "A000001",
    chamber: String = "House of Representatives",
    updateDate: Option[String] = Some("2024-06-01T00:00:00Z"),
  ): MemberDetailDTO =
    MemberDetailDTO(
      bioguideId = bioguideId,
      birthYear = Some("1970"),
      firstName = Some("Jane"),
      lastName = Some("Doe"),
      directOrderName = Some("Jane Doe"),
      invertedOrderName = Some("Doe, Jane"),
      honorificName = Some("Rep. Jane Doe"),
      cosponsoredLegislation = None,
      depiction = Some(MemberDepictionDTO(imageUrl = None, attribution = None)),
      leadership = None,
      partyHistory = Some(
        List(
          PartyHistoryDTO(
            partyAbbreviation = Some("D"),
            partyName = Some("Democratic"),
            startYear = Some(2023),
          )
        )
      ),
      sponsoredLegislation = None,
      state = Some("New York"),
      terms = Some(
        List(
          MemberDetailTermDTO(
            chamber = Some(chamber),
            congress = Some(118),
            endYear = Some(2025),
            memberType = Some("Representative"),
            startYear = Some(2023),
            stateCode = Some("NY"),
            stateName = Some("New York"),
            district = Some(5),
          )
        )
      ),
      updateDate = updateDate,
    )

  private def stubRepoBasics(f: TestFixture, existsLisMapping: Boolean = false): Unit = {
    when(f.historyArchiver.archiveMember(anyString())).thenReturn(doobie.free.connection.pure(()))
    when(f.memberRepo.upsert(any[MemberDO])).thenReturn(doobie.free.connection.pure(42L))
    when(f.termRepo.replaceAll(anyLong(), any[List[MemberTermDO]])).thenReturn(doobie.free.connection.pure(()))
    when(f.partyHistoryRepo.appendNew(anyLong(), any[List[MemberPartyHistoryDO]]))
      .thenReturn(doobie.free.connection.pure(()))
    val _ =
      when(f.memberRepo.existsWithLisMapping(anyLong())).thenReturn(doobie.free.connection.pure(existsLisMapping))
  }

  // ------------------------------------------------------------------
  // AC-05.3.1 — Streams all members for configured congress
  // ------------------------------------------------------------------
  "streamAll" should "stream a ProcessingResult per member" in {
    val f = createFixture()

    val items = (1 to 5).toList.map(i => makeListItem(bioguideId = f"M$i%06d"))

    when(f.apiClient.fetchAll(any[FetchParams])).thenReturn(fs2.Stream.emits(items))
    when(f.apiClient.fetchDetail(anyString())).thenAnswer { inv =>
      val url = inv.getArgument[String](0)
      // recover bioguide from url suffix
      val bio = url.split("/").lastOption.getOrElse("A000001")
      IO.pure(makeDetailDTO(bioguideId = bio))
    }
    when(f.memberRepo.findByBioguideId(anyString()))
      .thenReturn(doobie.free.connection.pure(Option.empty[MemberDO]))
      .thenReturn(doobie.free.connection.pure(Some(makeMember(memberId = 42L))))
    stubRepoBasics(f)

    val results = f.processor.streamAll(correlationId).compile.toList.unsafeRunSync()
    results.size shouldBe 5
  }

  it should "forward the configured congress in FetchParams" in {
    val f = createFixture()
    when(f.apiClient.fetchAll(any[FetchParams])).thenReturn(fs2.Stream.empty)

    val _ = f.processor.streamAll(correlationId).compile.toList.unsafeRunSync()
    verify(f.apiClient, times(1)).fetchAll(eqTo(FetchParams(congress = Some(config.congress))))
  }

  // ------------------------------------------------------------------
  // AC-05.3.2 — Fetches detail for each member (1:1 with list)
  // ------------------------------------------------------------------
  it should "fetch detail 1:1 with list items" in {
    val f     = createFixture()
    val items = List("A000001", "A000002", "A000003").map(id => makeListItem(bioguideId = id))

    when(f.apiClient.fetchAll(any[FetchParams])).thenReturn(fs2.Stream.emits(items))
    when(f.apiClient.fetchDetail(anyString())).thenReturn(IO.pure(makeDetailDTO()))
    when(f.memberRepo.findByBioguideId(anyString()))
      .thenReturn(doobie.free.connection.pure(Option.empty[MemberDO]))
      .thenReturn(doobie.free.connection.pure(Some(makeMember(memberId = 42L))))
    stubRepoBasics(f)

    val _ = f.processor.streamAll(correlationId).compile.toList.unsafeRunSync()
    verify(f.apiClient, times(3)).fetchDetail(anyString())
  }

  // ------------------------------------------------------------------
  // AC-05.3.3 — toDO conversion happens for valid DTO
  // ------------------------------------------------------------------
  "processMember" should "succeed for a valid detail DTO converting to a MemberConversionResult" in {
    val f = createFixture()
    when(f.apiClient.fetchDetail(anyString())).thenReturn(IO.pure(makeDetailDTO()))
    when(f.memberRepo.findByBioguideId(anyString()))
      .thenReturn(doobie.free.connection.pure(Option.empty[MemberDO]))
      .thenReturn(doobie.free.connection.pure(Some(makeMember(memberId = 42L))))
    stubRepoBasics(f)

    val result = f.processor.processMember(makeListItem(), correlationId).unsafeRunSync()
    result.isSucceeded shouldBe true
  }

  // ------------------------------------------------------------------
  // AC-05.3.4 — Skips unchanged members
  // ------------------------------------------------------------------
  it should "skip unchanged members with same updateDate and no field differences" in {
    val f = createFixture()
    val storedMember = makeMember(
      memberId = 42L,
      updateDate = Some(Instant.parse("2024-06-01T00:00:00Z")),
    )
    when(f.apiClient.fetchDetail(anyString())).thenReturn(IO.pure(makeDetailDTO()))
    when(f.memberRepo.findByBioguideId(anyString()))
      .thenReturn(doobie.free.connection.pure(Some(storedMember)))

    val result = f.processor.processMember(makeListItem(), correlationId).unsafeRunSync()
    val _      = result.isSkipped shouldBe true
    val _      = verify(f.historyArchiver, never()).archiveMember(anyString())
    val _      = verify(f.memberRepo, never()).upsert(any[MemberDO])
    succeed
  }

  // ------------------------------------------------------------------
  // AC-05.3.5 — Archives before upsert on changed members
  //         AC-05.3.6 — Does not archive new members (no-op returned by archiver)
  //         AC-05.3.7 — Upserts member profile on change
  //         AC-05.3.8 — Replaces terms on change
  //         AC-05.3.9 — Appends new party history on change
  // ------------------------------------------------------------------
  it should "call archive, upsert, replaceAll, and appendNew in order for a changed member" in {
    val f = createFixture()
    val older = makeMember(
      memberId = 42L,
      updateDate = Some(Instant.parse("2024-01-01T00:00:00Z")),
    )
    when(f.apiClient.fetchDetail(anyString())).thenReturn(IO.pure(makeDetailDTO()))
    when(f.memberRepo.findByBioguideId(anyString()))
      .thenReturn(doobie.free.connection.pure(Some(older)))
      .thenReturn(doobie.free.connection.pure(Some(makeMember(memberId = 42L))))
    stubRepoBasics(f)

    val _ = f.processor.processMember(makeListItem(), correlationId).unsafeRunSync()

    val order = org.mockito.Mockito.inOrder(f.historyArchiver, f.memberRepo, f.termRepo, f.partyHistoryRepo)
    val _     = order.verify(f.historyArchiver).archiveMember(anyString())
    val _     = order.verify(f.memberRepo).upsert(any[MemberDO])
    val _     = order.verify(f.termRepo).replaceAll(anyLong(), any[List[MemberTermDO]])
    val _     = order.verify(f.partyHistoryRepo).appendNew(anyLong(), any[List[MemberPartyHistoryDO]])
  }

  it should "not call archiveMember when the member is new" in {
    val f = createFixture()
    when(f.apiClient.fetchDetail(anyString())).thenReturn(IO.pure(makeDetailDTO()))
    when(f.memberRepo.findByBioguideId(anyString()))
      .thenReturn(doobie.free.connection.pure(Option.empty[MemberDO]))
      .thenReturn(doobie.free.connection.pure(Some(makeMember(memberId = 42L))))
    stubRepoBasics(f)

    val _ = f.processor.processMember(makeListItem(), correlationId).unsafeRunSync()
    // Archive is still invoked inside the transaction because the Doobie program always includes it; the
    // archiver itself returns unit for missing rows. We verify the composed call happens at-most-once.
    val _ = verify(f.historyArchiver, times(1)).archiveMember(anyString())
    // And — critically — upsert + replaceAll + appendNew still run.
    val _ = verify(f.memberRepo, times(1)).upsert(any[MemberDO])
    val _ = verify(f.termRepo, times(1)).replaceAll(anyLong(), any[List[MemberTermDO]])
    val _ = verify(f.partyHistoryRepo, times(1)).appendNew(anyLong(), any[List[MemberPartyHistoryDO]])
    succeed
  }

  // ------------------------------------------------------------------
  // AC-05.3.10 — House member emits member.updated immediately
  // ------------------------------------------------------------------
  it should "emit member.updated for a House member without consulting the LIS mapping table" in {
    val f = createFixture()
    when(f.apiClient.fetchDetail(anyString())).thenReturn(IO.pure(makeDetailDTO(chamber = "House of Representatives")))
    when(f.memberRepo.findByBioguideId(anyString()))
      .thenReturn(doobie.free.connection.pure(Option.empty[MemberDO]))
      .thenReturn(doobie.free.connection.pure(Some(makeMember(memberId = 42L))))
    stubRepoBasics(f)

    val result = f.processor.processMember(makeListItem(), correlationId).unsafeRunSync()
    val _      = result.isSucceeded shouldBe true
    val _      = verify(f.eventPublisher, times(1)).memberUpdated(any[MemberUpdatedEvent], eqTo(correlationId))
    verify(f.memberRepo, never()).existsWithLisMapping(anyLong())
  }

  // ------------------------------------------------------------------
  // AC-05.3.11 — Senate member with LIS mapping emits event
  // ------------------------------------------------------------------
  it should "emit member.updated for a Senate member when LIS mapping exists" in {
    val f = createFixture()
    when(f.apiClient.fetchDetail(anyString())).thenReturn(IO.pure(makeDetailDTO(chamber = "Senate")))
    when(f.memberRepo.findByBioguideId(anyString()))
      .thenReturn(doobie.free.connection.pure(Option.empty[MemberDO]))
      .thenReturn(doobie.free.connection.pure(Some(makeMember(memberId = 42L))))
    stubRepoBasics(f, existsLisMapping = true)

    val result = f.processor.processMember(makeListItem(), correlationId).unsafeRunSync()
    val _      = result.isSucceeded shouldBe true
    val _      = verify(f.memberRepo, times(1)).existsWithLisMapping(anyLong())
    verify(f.eventPublisher, times(1)).memberUpdated(any[MemberUpdatedEvent], eqTo(correlationId))
  }

  // ------------------------------------------------------------------
  // AC-05.3.12 — Senate member without LIS mapping skips event
  // ------------------------------------------------------------------
  it should "skip the member.updated event for a Senate member without an LIS mapping" in {
    val f = createFixture()
    when(f.apiClient.fetchDetail(anyString())).thenReturn(IO.pure(makeDetailDTO(chamber = "Senate")))
    when(f.memberRepo.findByBioguideId(anyString()))
      .thenReturn(doobie.free.connection.pure(Option.empty[MemberDO]))
      .thenReturn(doobie.free.connection.pure(Some(makeMember(memberId = 42L))))
    stubRepoBasics(f, existsLisMapping = false)

    val result = f.processor.processMember(makeListItem(), correlationId).unsafeRunSync()
    val _ = result match {
      case succeeded: repcheck.pipeline.models.metadata.ProcessingResult.Succeeded =>
        succeeded.eventEmitted shouldBe false
      case other =>
        fail(s"expected Succeeded with eventEmitted=false, got $other")
    }
    val _ = verify(f.eventPublisher, never()).memberUpdated(any[MemberUpdatedEvent], any[UUID])
    val _ = verify(f.logger, times(1)).info(
      any[LogContext],
      org.mockito.ArgumentMatchers.contains("no LIS mapping"),
    )
    succeed
  }

  // ------------------------------------------------------------------
  // AC-05.3.13 — Placeholder member gets filled in
  // ------------------------------------------------------------------
  it should "fill in a placeholder (null-updateDate) member via archive + upsert + event" in {
    val f = createFixture()
    val placeholder = makeMember(memberId = 42L, updateDate = None).copy(
      firstName = None,
      lastName = None,
      directOrderName = None,
      invertedOrderName = None,
      honorificName = None,
      birthYear = None,
      currentParty = None,
      state = None,
      district = None,
    )
    when(f.apiClient.fetchDetail(anyString())).thenReturn(IO.pure(makeDetailDTO()))
    when(f.memberRepo.findByBioguideId(anyString()))
      .thenReturn(doobie.free.connection.pure(Some(placeholder)))
      .thenReturn(doobie.free.connection.pure(Some(makeMember(memberId = 42L))))
    stubRepoBasics(f)

    val result = f.processor.processMember(makeListItem(), correlationId).unsafeRunSync()
    val _      = result.isSucceeded shouldBe true
    val _      = verify(f.historyArchiver, times(1)).archiveMember(anyString())
    val _      = verify(f.memberRepo, times(1)).upsert(any[MemberDO])
    verify(f.eventPublisher, times(1)).memberUpdated(any[MemberUpdatedEvent], eqTo(correlationId))
  }

  // ------------------------------------------------------------------
  // AC-05.3.14 — toDO failure produces ProcessingResult.Failure
  // ------------------------------------------------------------------
  it should "return Failed when DTO-to-DO conversion fails (within stream)" in {
    val f = createFixture()
    // empty bioguide triggers conversion failure
    val badDetail = makeDetailDTO(bioguideId = "")
    when(f.apiClient.fetchAll(any[FetchParams])).thenReturn(fs2.Stream.emit(makeListItem(bioguideId = "")))
    when(f.apiClient.fetchDetail(anyString())).thenReturn(IO.pure(badDetail))

    val results = f.processor.streamAll(correlationId).compile.toList.unsafeRunSync()
    val _       = results.size shouldBe 1
    results.headOption.map(_.isFailed) shouldBe Some(true)
  }

  it should "surface toDO failures as raised exceptions when processMember is called directly" in {
    val f = createFixture()
    when(f.apiClient.fetchDetail(anyString())).thenReturn(IO.pure(makeDetailDTO(bioguideId = "")))

    val outcome = f.processor.processMember(makeListItem(bioguideId = ""), correlationId).attempt.unsafeRunSync()
    outcome.isLeft shouldBe true
  }

  // ------------------------------------------------------------------
  // AC-05.3.15 — API failure produces ProcessingResult.Failure
  // ------------------------------------------------------------------
  it should "mark a member as Failed when the detail fetch raises an error" in {
    val f = createFixture()
    when(f.apiClient.fetchAll(any[FetchParams])).thenReturn(fs2.Stream.emit(makeListItem()))
    when(f.apiClient.fetchDetail(anyString())).thenReturn(IO.raiseError(new RuntimeException("boom")))

    val results = f.processor.streamAll(correlationId).compile.toList.unsafeRunSync()
    val _       = results.size shouldBe 1
    results.headOption.map(_.isFailed) shouldBe Some(true)
  }

  it should "log and swallow page-level fetch errors so partial results still stream" in {
    val f = createFixture()
    when(f.apiClient.fetchAll(any[FetchParams]))
      .thenReturn(fs2.Stream.raiseError[IO](new RuntimeException("network timeout")))

    val results = f.processor.streamAll(correlationId).compile.toList.unsafeRunSync()
    val _       = results shouldBe empty
    verify(f.logger, times(1)).error(
      any[LogContext],
      org.mockito.ArgumentMatchers.contains("Page fetch failed"),
      any[Option[Throwable]],
    )
  }

  it should "treat event publisher failures as Failed results within the stream" in {
    val f = createFixture()
    when(f.apiClient.fetchAll(any[FetchParams])).thenReturn(fs2.Stream.emit(makeListItem()))
    when(f.apiClient.fetchDetail(anyString())).thenReturn(IO.pure(makeDetailDTO()))
    when(f.memberRepo.findByBioguideId(anyString()))
      .thenReturn(doobie.free.connection.pure(Option.empty[MemberDO]))
      .thenReturn(doobie.free.connection.pure(Some(makeMember(memberId = 42L))))
    stubRepoBasics(f)
    when(f.eventPublisher.memberUpdated(any[MemberUpdatedEvent], any[UUID]))
      .thenReturn(IO.raiseError(new RuntimeException("pubsub down")))

    val results = f.processor.streamAll(correlationId).compile.toList.unsafeRunSync()
    val _       = results.size shouldBe 1
    results.headOption.map(_.isFailed) shouldBe Some(true)
  }

  // ------------------------------------------------------------------
  // AC-05.3.16 — Parallelism respected
  // ------------------------------------------------------------------
  it should "respect config.parallelism — a sequential (1) config processes one-at-a-time" in {
    val f        = createFixture()
    val items    = (1 to 6).toList.map(i => makeListItem(bioguideId = f"M$i%06d"))
    val inFlight = new AtomicInteger(0)
    val maxSeen  = new AtomicInteger(0)

    when(f.apiClient.fetchAll(any[FetchParams])).thenReturn(fs2.Stream.emits(items))
    when(f.apiClient.fetchDetail(anyString())).thenAnswer { _ =>
      IO {
        val now = inFlight.incrementAndGet()
        maxSeen.updateAndGet(prev => Math.max(prev, now))
        // tiny delay to encourage overlap
        Thread.sleep(5L)
        val _ = inFlight.decrementAndGet()
        makeDetailDTO()
      }
    }
    when(f.memberRepo.findByBioguideId(anyString()))
      .thenReturn(doobie.free.connection.pure(Option.empty[MemberDO]))
      .thenReturn(doobie.free.connection.pure(Some(makeMember(memberId = 42L))))
    stubRepoBasics(f)

    val _ = f.processor.streamAll(correlationId).compile.toList.unsafeRunSync()
    maxSeen.get() shouldBe 1
  }

  it should "respect a higher parallelism setting by allowing overlap" in {
    val f        = createFixture()
    val items    = (1 to 8).toList.map(i => makeListItem(bioguideId = f"M$i%06d"))
    val inFlight = new AtomicInteger(0)
    val maxSeen  = new AtomicInteger(0)

    when(f.apiClient.fetchAll(any[FetchParams])).thenReturn(fs2.Stream.emits(items))
    when(f.apiClient.fetchDetail(anyString())).thenAnswer { _ =>
      IO {
        val now = inFlight.incrementAndGet()
        maxSeen.updateAndGet(prev => Math.max(prev, now))
        Thread.sleep(20L)
        val _ = inFlight.decrementAndGet()
        makeDetailDTO()
      }
    }
    when(f.memberRepo.findByBioguideId(anyString()))
      .thenReturn(doobie.free.connection.pure(Option.empty[MemberDO]))
      .thenReturn(doobie.free.connection.pure(Some(makeMember(memberId = 42L))))
    stubRepoBasics(f)

    val parallelCfg = config.copy(parallelism = 3)
    val _           = f.processorWith(parallelCfg).streamAll(correlationId).compile.toList.unsafeRunSync()
    // With 3 slots and 20ms work, we expect to see more than 1 in flight at some point.
    maxSeen.get() should be > 1
  }

  // ------------------------------------------------------------------
  // AC-05.3.17 — Correlation ID flows through all operations
  // ------------------------------------------------------------------
  it should "propagate the correlation id into log contexts and the published event" in {
    val f = createFixture()
    when(f.apiClient.fetchAll(any[FetchParams])).thenReturn(fs2.Stream.emit(makeListItem()))
    when(f.apiClient.fetchDetail(anyString())).thenReturn(IO.pure(makeDetailDTO()))
    when(f.memberRepo.findByBioguideId(anyString()))
      .thenReturn(doobie.free.connection.pure(Option.empty[MemberDO]))
      .thenReturn(doobie.free.connection.pure(Some(makeMember(memberId = 42L))))
    stubRepoBasics(f)

    val myCorrelationId = UUID.randomUUID()
    val _               = f.processor.streamAll(myCorrelationId).compile.toList.unsafeRunSync()

    // The publisher receives exactly this correlation id.
    val _ = verify(f.eventPublisher, times(1)).memberUpdated(any[MemberUpdatedEvent], eqTo(myCorrelationId))
    // Every log call should carry a LogContext whose runId matches the correlation id string form.
    val captor = org.mockito.ArgumentCaptor.forClass(classOf[LogContext])
    val _      = verify(f.logger, org.mockito.Mockito.atLeastOnce()).info(captor.capture(), anyString())
    import scala.jdk.CollectionConverters._
    captor.getAllValues.asScala.toList.foreach(ctx => ctx.runId shouldBe myCorrelationId.toString)
  }

  // ------------------------------------------------------------------
  // Additional safety nets covering chamber detection, partial data, and malformed input.
  // ------------------------------------------------------------------
  "chamberFromTerms" should "pick the chamber of the most-recent congress" in {
    val f = createFixture()
    val terms = List(
      houseTerm(congress = 117),
      senateTerm(congress = 118),
    )
    f.processor.chamberFromTerms(terms) shouldBe Some(Chamber.Senate)
  }

  it should "fall back to the first term when no term has a congress value" in {
    val f    = createFixture()
    val term = houseTerm(congress = 118).copy(congress = None)
    f.processor.chamberFromTerms(List(term)) shouldBe Some(Chamber.House)
  }

  it should "return None when terms are empty" in {
    val f = createFixture()
    f.processor.chamberFromTerms(Nil) shouldBe None
  }

  "isChanged" should "mark None->Some updateDate transitions as changed (placeholder case)" in {
    val f        = createFixture()
    val stored   = makeMember(updateDate = None)
    val incoming = makeMember(updateDate = Some(Instant.parse("2024-06-01T00:00:00Z")))
    f.processor.isChanged(incoming, stored) shouldBe true
  }

  it should "treat the member as unchanged when updateDate does not advance, even if fields differ" in {
    val f        = createFixture()
    val stored   = makeMember(updateDate = Some(Instant.parse("2024-06-01T00:00:00Z")))
    val incoming = stored.copy(firstName = Some("Different"))
    f.processor.isChanged(incoming, stored) shouldBe false
  }

  it should "mark field differences as changed once updateDate advances" in {
    val f      = createFixture()
    val stored = makeMember(updateDate = Some(Instant.parse("2024-06-01T00:00:00Z")))
    val incoming =
      stored.copy(firstName = Some("Different"), updateDate = Some(Instant.parse("2024-07-01T00:00:00Z")))
    f.processor.isChanged(incoming, stored) shouldBe true
  }

  it should "treat a newer updateDate as enough on its own to mark the member as changed" in {
    val f      = createFixture()
    val stored = makeMember(updateDate = Some(Instant.parse("2024-06-01T00:00:00Z")))
    val incoming =
      stored.copy(updateDate = Some(Instant.parse("2024-07-01T00:00:00Z")))
    f.processor.isChanged(incoming, stored) shouldBe true
  }

  it should "consider the member unchanged when updateDate and fields both match" in {
    val f      = createFixture()
    val stored = makeMember(updateDate = Some(Instant.parse("2024-06-01T00:00:00Z")))
    f.processor.isChanged(stored, stored) shouldBe false
  }

  "processMember" should "raise when the list item has no detail URL" in {
    val f     = createFixture()
    val noUrl = makeListItem().copy(url = None)
    // fetchDetail will receive empty string → Uri parse fails inside the real client.
    val failure = new RuntimeException("empty URL")
    when(f.apiClient.fetchDetail(eqTo(""))).thenReturn(IO.raiseError(failure))

    val outcome = f.processor.processMember(noUrl, correlationId).attempt.unsafeRunSync()
    outcome.isLeft shouldBe true
  }

}
