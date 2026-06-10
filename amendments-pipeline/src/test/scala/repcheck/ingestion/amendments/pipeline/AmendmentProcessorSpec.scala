package repcheck.ingestion.amendments.pipeline

import java.time.Instant
import java.util.UUID
import java.util.concurrent.atomic.AtomicReference

import scala.concurrent.duration._

import cats.effect.IO
import cats.effect.unsafe.implicits.global

import doobie._
import doobie.util.transactor.Strategy

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar
import repcheck.ingestion.amendments.api.AmendmentsApiClient
import repcheck.ingestion.amendments.config.AmendmentsConfig
import repcheck.ingestion.amendments.observability.AmendmentMetrics
import repcheck.ingestion.amendments.persistence.{AmendmentRepository, CommitteeLookupRepository}
import repcheck.ingestion.bills.common.persistence.BillRepository
import repcheck.ingestion.common.api.{FetchParams, PagedResponse}
import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}
import repcheck.ingestion.common.placeholders.{EntityRepository, PlaceholderCreator}
import repcheck.members.common.persistence.MemberRepository
import repcheck.pipeline.models.metadata.ProcessingResult
import repcheck.shared.models.congress.amendment.AmendmentType
import repcheck.shared.models.congress.bill.TextVersionCode
import repcheck.shared.models.congress.common.{Chamber, Party, UsState}
import repcheck.shared.models.congress.dos.amendment.AmendmentDO
import repcheck.shared.models.congress.dos.bill.BillDO
import repcheck.shared.models.congress.dos.member.MemberDO
import repcheck.shared.models.congress.dto.amendment.{
  AmendedAmendmentDTO,
  AmendedBillDTO,
  AmendmentDetailDTO,
  AmendmentListItemDTO,
}
import repcheck.shared.models.congress.dto.bill.{LatestActionDTO, SponsorDTO}
import repcheck.shared.models.placeholder.HasPlaceholder

import com.repcheck.utils.errors.{RetryConfig, RetryWrapper}

/**
 * Unit coverage for [[AmendmentProcessor]]. The api client is mocked via Mockito so we don't have to thread real
 * `Client[IO]` / `RetryWrapper[IO]` instances; the repositories use trait-implementation stubs backed by
 * `AtomicReference`s so individual recursion-frame calls are observable. The transactor uses an in-memory H2 with
 * `Strategy.void` so `ConnectionIO.pure` programs round-trip without hitting a real driver.
 */
class AmendmentProcessorSpec extends AnyFlatSpec with Matchers with MockitoSugar {

  // --------------------------------------------------------------------------------------------
  // Helpers
  // --------------------------------------------------------------------------------------------

  private val noopXa: Transactor[IO] = {
    val xa = Transactor.fromDriverManager[IO](
      driver = "org.h2.Driver",
      url = "jdbc:h2:mem:noop;DB_CLOSE_DELAY=-1",
      user = "",
      password = "",
      logHandler = None,
    )
    Transactor.strategy.set(xa, Strategy.void)
  }

  private def silentLogger(): PipelineLogger[IO] = new PipelineLogger[IO] {
    override def info(context: LogContext, message: String): IO[Unit]                            = IO.unit
    override def warn(context: LogContext, message: String): IO[Unit]                            = IO.unit
    override def error(context: LogContext, message: String, cause: Option[Throwable]): IO[Unit] = IO.unit
    override def debug(context: LogContext, message: String): IO[Unit]                           = IO.unit
  }

  private val testCorrelationId = UUID.fromString("00000000-0000-0000-0000-000000000010")

  private val baseConfig = AmendmentsConfig(
    congressesMin = 117,
    congressesMax = 117,
    lookbackDays = 7,
    parallelism = 2,
    pageDelay = 0.millis,
    maxRecursionDepth = 3,
    pageSize = 250,
  )

  private def detail(
    congress: Int = 117,
    amendmentType: String = "SAMDT",
    number: String = "100",
    sponsorBioguide: Option[String] = Some("S001217"),
    committeeSponsorUrl: Option[String] = None,
    amendedBill: Option[AmendedBillDTO] = None,
    amendedAmendment: Option[AmendedAmendmentDTO] = None,
    chamber: Option[String] = Some("Senate"),
    updateDate: Option[String] = Some("2024-06-01T12:00:00Z"),
  ): AmendmentDetailDTO =
    AmendmentDetailDTO(
      congress = congress,
      number = number,
      amendmentType = Some(amendmentType),
      amendedBill = amendedBill,
      amendedAmendment = amendedAmendment,
      chamber = chamber,
      description = Some("Test"),
      purpose = Some("Test purpose"),
      sponsors = committeeSponsorUrl
        .map(u => List(SponsorDTO.CommitteeSponsorDTO(name = "Test Committee", url = u)))
        .orElse(sponsorBioguide.map { id =>
          List(
            SponsorDTO.MemberSponsorDTO(
              bioguideId = id,
              firstName = Some("Test"),
              lastName = Some("Senator"),
              fullName = Some("Test Senator"),
              middleName = None,
              isByRequest = None,
              party = Some("D"),
              state = Some("NY"),
              district = None,
              url = None,
            )
          )
        }),
      submittedDate = Some("2021-08-01"),
      proposedDate = Some("2021-08-02"),
      latestAction = Some(
        LatestActionDTO(
          actionDate = "2024-06-01",
          text = "Action text.",
          actionTime = Some("12:00:00"),
        )
      ),
      updateDate = updateDate,
      actions = None,
      textVersions = None,
      cosponsors = None,
      amendmentsToAmendment = None,
    )

  private def listItem(
    congress: Int = 117,
    amendmentType: String = "SAMDT",
    number: String = "100",
    updateDate: Option[String] = Some("2024-06-01T12:00:00Z"),
  ): AmendmentListItemDTO =
    AmendmentListItemDTO(
      congress = congress,
      number = number,
      amendmentType = Some(amendmentType),
      description = None,
      latestAction = None,
      updateDate = updateDate,
      url = Some(
        s"https://api.congress.gov/v3/amendment/${congress.toString}/${amendmentType.toLowerCase}/$number"
      ),
    )

  private def storedAmendment(
    naturalKey: String = "117-SAMDT-100",
    updateDate: Option[Instant],
    billId: Option[Long] = None,
  ): AmendmentDO =
    AmendmentDO(
      amendmentId = 5000L,
      naturalKey = naturalKey,
      congress = 117,
      amendmentType = Some(AmendmentType.SAMDT),
      number = "100",
      billId = billId,
      chamber = Chamber.Senate,
      description = None,
      purpose = None,
      sponsorMemberId = None,
      sponsorCommitteeId = None,
      sponsorType = None,
      submittedDate = None,
      proposedDate = None,
      latestActionDate = None,
      latestActionTime = None,
      latestActionText = None,
      updateDate = updateDate,
      apiUrl = None,
      parentAmendmentId = None,
      lastTextCheckAt = None,
      createdAt = None,
      updatedAt = None,
    )

  // --------------------------------------------------------------------------------------------
  // Stubs (trait implementations backed by Atomic state)
  // --------------------------------------------------------------------------------------------

  private class StubAmendmentRepo(
    initial: Map[String, AmendmentDO]
  ) extends AmendmentRepository[ConnectionIO] {

    val storeRef = new AtomicReference[Map[String, AmendmentDO]](initial)
    val findByNaturalKeysCallsRef: AtomicReference[List[List[String]]] = new AtomicReference(List.empty)
    val upsertCallsRef: AtomicReference[List[AmendmentDO]]             = new AtomicReference(List.empty)

    override def upsert(amendment: AmendmentDO): ConnectionIO[AmendmentDO] = {
      val _     = upsertCallsRef.updateAndGet(_ :+ amendment)
      val saved = amendment.copy(amendmentId = idForKey(amendment.naturalKey))
      val _     = storeRef.updateAndGet(_ + (amendment.naturalKey -> saved))
      doobie.free.connection.pure(saved)
    }

    override def upsertPlaceholder(naturalKey: String): ConnectionIO[Unit] = doobie.free.connection.pure(())

    override def insertIfNotExists(entity: AmendmentDO): ConnectionIO[Unit] = doobie.free.connection.pure(())

    override def findById(id: Long): ConnectionIO[Option[AmendmentDO]] =
      doobie.free.connection.pure(storeRef.get().values.find(_.amendmentId == id))

    override def findByNaturalKey(naturalKey: String): ConnectionIO[Option[AmendmentDO]] =
      doobie.free.connection.pure(storeRef.get().get(naturalKey))

    override def findByNaturalKeys(naturalKeys: List[String]): ConnectionIO[Map[String, AmendmentDO]] = {
      val _   = findByNaturalKeysCallsRef.updateAndGet(_ :+ naturalKeys)
      val map = storeRef.get()
      doobie.free.connection.pure(naturalKeys.flatMap(k => map.get(k).map(k -> _)).toMap)
    }

    override def findByBillId(billId: Long): ConnectionIO[List[AmendmentDO]] =
      doobie.free.connection.pure(storeRef.get().values.filter(_.billId.contains(billId)).toList)

    override def findByCongress(congress: Int): ConnectionIO[List[AmendmentDO]] =
      doobie.free.connection.pure(storeRef.get().values.filter(_.congress == congress).toList)

    override def findByParentAmendmentId(parentId: Long): ConnectionIO[List[AmendmentDO]] =
      doobie.free.connection.pure(storeRef.get().values.filter(_.parentAmendmentId.contains(parentId)).toList)

    override def findCandidatesForTextCheck(
      minCongress: Int,
      staleAfter: FiniteDuration,
    ): ConnectionIO[List[AmendmentDO]] = doobie.free.connection.pure(List.empty)

    override def updateLastTextCheckAt(amendmentId: Long): ConnectionIO[Unit] =
      doobie.free.connection.pure(())

    private def idForKey(nk: String): Long = math.abs(nk.hashCode.toLong)
  }

  private class StubBillRepo extends BillRepository[ConnectionIO] {

    val storeRef                                           = new AtomicReference[Map[String, Long]](Map.empty)
    val placeholderCallsRef: AtomicReference[List[String]] = new AtomicReference(List.empty)

    override def upsert(bill: BillDO): ConnectionIO[Long] = doobie.free.connection.pure(0L)

    override def findByBillId(billId: String): ConnectionIO[Option[BillDO]] =
      doobie.free.connection.pure(storeRef.get().get(billId).map(id => fakeBill(billId, id)))

    override def findByBillIds(billIds: List[String]): ConnectionIO[List[BillDO]] =
      doobie.free.connection.pure(billIds.flatMap(k => storeRef.get().get(k).map(id => fakeBill(k, id))))

    override def findBillsNeedingTextCheck(congresses: List[Int]): ConnectionIO[List[BillDO]] =
      doobie.free.connection.pure(List.empty)

    override def updateTextFields(
      billId: String,
      textVersionType: String,
      latestTextVersionId: Long,
    ): ConnectionIO[Unit] = doobie.free.connection.pure(())

    override def upsertPlaceholder(naturalKey: String): ConnectionIO[Unit] = {
      val _ = placeholderCallsRef.updateAndGet(_ :+ naturalKey)
      val _ = storeRef.updateAndGet(m => m + (naturalKey -> math.abs(naturalKey.hashCode.toLong)))
      doobie.free.connection.pure(())
    }

    override def findExpectedVersion(naturalKey: String): ConnectionIO[Option[TextVersionCode]] =
      doobie.free.connection.pure(None)

    override def updateExpectedVersion(naturalKey: String, code: TextVersionCode): ConnectionIO[Unit] =
      doobie.free.connection.pure(())

    private def fakeBill(naturalKey: String, id: Long): BillDO =
      BillDO(
        billId = id,
        naturalKey = naturalKey,
        congress = 117,
        billType = repcheck.shared.models.congress.common.BillType.HR,
        number = "1",
        title = "",
        originChamber = None,
        originChamberCode = None,
        introducedDate = None,
        policyArea = None,
        latestActionDate = None,
        latestActionText = None,
        constitutionalAuthorityText = None,
        sponsorMemberId = None,
        textVersionType = None,
        updateDate = None,
        updateDateIncludingText = None,
        legislationUrl = None,
        apiUrl = None,
        createdAt = None,
        updatedAt = None,
        latestTextVersionId = None,
      )

  }

  private class StubCommitteeLookupRepo extends CommitteeLookupRepository[ConnectionIO] {

    val storeRef                                    = new AtomicReference[Map[String, Long]](Map.empty)
    val findCallsRef: AtomicReference[List[String]] = new AtomicReference(List.empty)

    override def findIdBySystemCode(systemCode: String): ConnectionIO[Option[Long]] = {
      val _ = findCallsRef.updateAndGet(_ :+ systemCode)
      doobie.free.connection.pure(storeRef.get().get(systemCode))
    }

  }

  private class StubMemberRepo extends MemberRepository {

    val storeRef                                    = new AtomicReference[Map[String, Long]](Map.empty)
    val findCallsRef: AtomicReference[List[String]] = new AtomicReference(List.empty)

    override def findById(memberId: Long): ConnectionIO[Option[MemberDO]] = doobie.free.connection.pure(None)

    override def findByBioguideId(bioguideId: String): ConnectionIO[Option[MemberDO]] = {
      val _ = findCallsRef.updateAndGet(_ :+ bioguideId)
      doobie.free.connection.pure(storeRef.get().get(bioguideId).map(id => fakeMember(bioguideId, id)))
    }

    override def upsert(member: MemberDO): ConnectionIO[Long] = doobie.free.connection.pure(0L)

    override def findPlaceholders(): ConnectionIO[List[MemberDO]] = doobie.free.connection.pure(List.empty)

    override def existsWithLisMapping(memberId: Long): ConnectionIO[Boolean] = doobie.free.connection.pure(false)

    private def fakeMember(bioguideId: String, id: Long): MemberDO =
      MemberDO(
        memberId = id,
        naturalKey = bioguideId,
        firstName = None,
        lastName = None,
        directOrderName = None,
        invertedOrderName = None,
        honorificName = None,
        birthYear = None,
        currentParty = Option.empty[Party],
        state = Option.empty[UsState],
        district = None,
        imageUrl = None,
        imageAttribution = None,
        officialUrl = None,
        updateDate = None,
        createdAt = None,
        updatedAt = None,
      )

  }

  private class StubPlaceholderCreator extends PlaceholderCreator[IO] {

    val callsRef: AtomicReference[List[String]] = new AtomicReference(List.empty)

    override def ensureExists[T <: Product](
      naturalKey: String,
      repository: EntityRepository[IO, T],
    )(using HasPlaceholder[T]): IO[Unit] = IO {
      val _ = callsRef.updateAndGet(_ :+ naturalKey)
    }

  }

  private class StubMemberEntityRepo extends EntityRepository[IO, MemberDO] {
    override def insertIfNotExists(entity: MemberDO): IO[Unit] = IO.unit
  }

  /**
   * Test-friendly subclass of [[AmendmentsApiClient]] whose `fetchPage` + `fetchDetail` are wired from in-memory maps.
   * Subclassing rather than `mock[]` so the inherited `fetchAll` keeps its real implementation (which depends on the
   * `protected def temporal` Mockito would otherwise return as `null`).
   */
  private case class ApiCall(detailUrl: String, correlationId: UUID)

  private class TestApiClient(
    detailMap: Map[String, AmendmentDetailDTO],
    pageItems: List[AmendmentListItemDTO],
    calls: AtomicReference[List[ApiCall]],
  ) extends AmendmentsApiClient[IO](
        config = repcheck.ingestion.common.api.CongressGovClientConfig(
          baseUrl = "http://localhost:0",
          apiKey = "test",
          pageSize = math.max(pageItems.size + 1, 1),
          pageDelay = Duration.Zero,
          retry = RetryConfig(),
        ),
        client = mock[org.http4s.client.Client[IO]],
        retryWrapper = new RetryWrapper[IO]((_, _, _, _, _, _) => IO.unit),
        temporalInstance = cats.effect.Temporal[IO],
      ) {

    override def fetchPage(params: FetchParams): IO[PagedResponse[AmendmentListItemDTO]] =
      IO.pure(
        PagedResponse[AmendmentListItemDTO](
          items = if (params.offset == 0) { pageItems }
          else { Nil },
          totalCount = pageItems.size,
          nextOffset = None,
        )
      )

    override def fetchDetail(detailUrl: String, correlationId: UUID): IO[AmendmentDetailDTO] = {
      val _ = calls.updateAndGet(_ :+ ApiCall(detailUrl, correlationId))
      // Tests key the stub map by natural key (`{congress}-{TYPE}-{number}`); the production processor now
      // hands us a URL like `https://api.congress.gov/v3/amendment/117/samdt/100`. Look up by URL first
      // (so a test can target a specific URL if it cares), then fall back to extracting the natural key
      // from the URL's last three segments. Pass `-1` to `split` so trailing empty segments survive — the
      // "bad detail" test relies on `.../samdt/` (empty number) round-tripping to `117-SAMDT-`.
      val nkFromUrl = detailUrl.split("/", -1).toList.takeRight(3) match {
        case List(c, t, n) => Some(s"$c-${t.toUpperCase}-$n")
        case _             => None
      }
      detailMap.get(detailUrl).orElse(nkFromUrl.flatMap(detailMap.get)) match {
        case Some(d) => IO.pure(d)
        case None    => IO.raiseError[AmendmentDetailDTO](new IllegalStateException(s"no stub detail for $detailUrl"))
      }
    }

  }

  private def mockApi(
    detailMap: Map[String, AmendmentDetailDTO] = Map.empty,
    pageItems: List[AmendmentListItemDTO] = Nil,
  ): (AmendmentsApiClient[IO], AtomicReference[List[ApiCall]]) = {
    val calls = new AtomicReference[List[ApiCall]](Nil)
    (new TestApiClient(detailMap, pageItems, calls), calls)
  }

  private def buildProcessor(
    api: AmendmentsApiClient[IO],
    amendmentRepo: StubAmendmentRepo,
    billRepo: StubBillRepo,
    memberRepo: StubMemberRepo,
    placeholderCreator: StubPlaceholderCreator,
    config: AmendmentsConfig = baseConfig,
    metrics: AmendmentMetrics = AmendmentMetrics.make(),
    committeeRepo: StubCommitteeLookupRepo = new StubCommitteeLookupRepo,
  ): AmendmentProcessor[IO] =
    new AmendmentProcessor[IO](
      apiClient = api,
      amendmentRepository = amendmentRepo,
      placeholderCreator = placeholderCreator,
      memberRepository = memberRepo,
      memberEntityRepo = new StubMemberEntityRepo,
      billRepository = billRepo,
      committeeRepository = committeeRepo,
      xa = noopXa,
      config = config,
      logger = silentLogger(),
      metrics = metrics,
    )

  // --------------------------------------------------------------------------------------------
  // Specs
  // --------------------------------------------------------------------------------------------

  "processAmendment" should "trip the depth guard at maxRecursionDepth + 1 and return Failed(AmendmentRecursionTooDeep)" in {
    val (api, calls) = mockApi()
    val aRepo        = new StubAmendmentRepo(Map.empty)
    val bRepo        = new StubBillRepo
    val mRepo        = new StubMemberRepo
    val placeholder  = new StubPlaceholderCreator
    val metrics      = AmendmentMetrics.make()
    val processor    = buildProcessor(api, aRepo, bRepo, mRepo, placeholder, metrics = metrics)

    val result = processor
      .processAmendment(
        naturalKey = "117-SAMDT-100",
        listItemOpt = Some(listItem()),
        detailUrlOpt = None,
        storedOpt = None,
        depth = baseConfig.maxRecursionDepth + 1,
        correlationId = testCorrelationId,
      )
      .unsafeRunSync()

    result match {
      case f: ProcessingResult.Failed =>
        val _ = f.entityId shouldBe "117-SAMDT-100"
        val _ = f.reason should include("recursion exceeded max depth")
        val _ = calls.get() shouldBe Nil
        metrics.snapshot().recursionDepthExceeded shouldBe 1L
      case other => fail(s"expected Failed, got $other")
    }
  }

  it should "skip with reason 'unchanged' when stored.updateDate is >= incoming list updateDate" in {
    val (api, calls) = mockApi()
    val aRepo        = new StubAmendmentRepo(Map.empty)
    val bRepo        = new StubBillRepo
    val mRepo        = new StubMemberRepo
    val placeholder  = new StubPlaceholderCreator
    val processor    = buildProcessor(api, aRepo, bRepo, mRepo, placeholder)

    val item   = listItem(updateDate = Some("2024-06-01T12:00:00Z"))
    val stored = storedAmendment(updateDate = Some(Instant.parse("2024-06-01T12:00:00Z")))

    val result = processor
      .processAmendment(
        naturalKey = "117-SAMDT-100",
        listItemOpt = Some(item),
        detailUrlOpt = None,
        storedOpt = Some(stored),
        depth = 0,
        correlationId = testCorrelationId,
      )
      .unsafeRunSync()

    val _ = result shouldBe ProcessingResult.Skipped("117-SAMDT-100", "unchanged")
    calls.get() shouldBe Nil
  }

  it should "fetch the detail and upsert when stored has older updateDate" in {
    val item         = listItem(updateDate = Some("2024-06-02T12:00:00Z"))
    val d            = detail()
    val (api, calls) = mockApi(Map("117-SAMDT-100" -> d))
    val aRepo        = new StubAmendmentRepo(Map.empty)
    val bRepo        = new StubBillRepo
    val mRepo        = new StubMemberRepo
    val placeholder  = new StubPlaceholderCreator
    val processor    = buildProcessor(api, aRepo, bRepo, mRepo, placeholder)

    val result = processor
      .processAmendment(
        naturalKey = "117-SAMDT-100",
        listItemOpt = Some(item),
        detailUrlOpt = None,
        storedOpt = Some(storedAmendment(updateDate = Some(Instant.parse("2024-05-01T00:00:00Z")))),
        depth = 0,
        correlationId = testCorrelationId,
      )
      .unsafeRunSync()

    val _ = result shouldBe ProcessingResult.Succeeded("117-SAMDT-100")
    val _ = calls.get().map(_.detailUrl) shouldBe List(
      "https://api.congress.gov/v3/amendment/117/samdt/100"
    )
    aRepo.upsertCallsRef.get().size shouldBe 1
  }

  it should "call PlaceholderCreator.ensureExists + memberRepo.findByBioguideId for sponsors" in {
    val sponsorId = "S001217"
    val d         = detail(sponsorBioguide = Some(sponsorId))
    val (api, _)  = mockApi(Map("117-SAMDT-100" -> d))
    val aRepo     = new StubAmendmentRepo(Map.empty)
    val bRepo     = new StubBillRepo
    val mRepo     = new StubMemberRepo
    mRepo.storeRef.set(Map(sponsorId -> 999L))
    val placeholder = new StubPlaceholderCreator
    val processor   = buildProcessor(api, aRepo, bRepo, mRepo, placeholder)

    val _ = processor
      .processAmendment(
        naturalKey = "117-SAMDT-100",
        listItemOpt = Some(listItem()),
        detailUrlOpt = None,
        storedOpt = None,
        depth = 0,
        correlationId = testCorrelationId,
      )
      .unsafeRunSync()

    val _ = placeholder.callsRef.get() shouldBe List(sponsorId)
    val _ = mRepo.findCallsRef.get() shouldBe List(sponsorId)
    aRepo.upsertCallsRef.get().headOption.flatMap(_.sponsorMemberId) shouldBe Some(999L)
  }

  it should "skip placeholderCreator + findByBioguideId when DTO has no sponsor" in {
    val d           = detail(sponsorBioguide = None)
    val (api, _)    = mockApi(Map("117-SAMDT-100" -> d))
    val aRepo       = new StubAmendmentRepo(Map.empty)
    val bRepo       = new StubBillRepo
    val mRepo       = new StubMemberRepo
    val placeholder = new StubPlaceholderCreator
    val processor   = buildProcessor(api, aRepo, bRepo, mRepo, placeholder)

    val _ = processor
      .processAmendment(
        naturalKey = "117-SAMDT-100",
        listItemOpt = Some(listItem()),
        detailUrlOpt = None,
        storedOpt = None,
        depth = 0,
        correlationId = testCorrelationId,
      )
      .unsafeRunSync()

    val _ = placeholder.callsRef.get() shouldBe Nil
    mRepo.findCallsRef.get() shouldBe Nil
  }

  it should "resolve sponsorCommitteeId from a committee-sponsor URL's systemCode" in {
    val d        = detail(committeeSponsorUrl = Some("https://api.congress.gov/v3/committee/house/hsru00?format=json"))
    val (api, _) = mockApi(Map("117-SAMDT-100" -> d))
    val aRepo    = new StubAmendmentRepo(Map.empty)
    val bRepo    = new StubBillRepo
    val mRepo    = new StubMemberRepo
    val cRepo    = new StubCommitteeLookupRepo
    cRepo.storeRef.set(Map("hsru00" -> 221L))
    val placeholder = new StubPlaceholderCreator
    val processor   = buildProcessor(api, aRepo, bRepo, mRepo, placeholder, committeeRepo = cRepo)

    val _ = processor
      .processAmendment(
        naturalKey = "117-SAMDT-100",
        listItemOpt = Some(listItem()),
        detailUrlOpt = None,
        storedOpt = None,
        depth = 0,
        correlationId = testCorrelationId,
      )
      .unsafeRunSync()

    val _      = cRepo.findCallsRef.get() shouldBe List("hsru00")
    val _      = mRepo.findCallsRef.get() shouldBe Nil // committee sponsor → member path skipped
    val stored = aRepo.upsertCallsRef.get().headOption
    val _      = stored.flatMap(_.sponsorCommitteeId) shouldBe Some(221L)
    val _      = stored.flatMap(_.sponsorMemberId) shouldBe None
    stored.flatMap(_.sponsorType.map(_.toString)) shouldBe Some("Committee")
  }

  it should "leave sponsorCommitteeId None when the committee systemCode has no matching row" in {
    val d        = detail(committeeSponsorUrl = Some("https://api.congress.gov/v3/committee/house/hsxx99?format=json"))
    val (api, _) = mockApi(Map("117-SAMDT-100" -> d))
    val aRepo    = new StubAmendmentRepo(Map.empty)
    val bRepo    = new StubBillRepo
    val mRepo    = new StubMemberRepo
    val cRepo    = new StubCommitteeLookupRepo // empty store → miss
    val placeholder = new StubPlaceholderCreator
    val processor   = buildProcessor(api, aRepo, bRepo, mRepo, placeholder, committeeRepo = cRepo)

    val _ = processor
      .processAmendment(
        naturalKey = "117-SAMDT-100",
        listItemOpt = Some(listItem()),
        detailUrlOpt = None,
        storedOpt = None,
        depth = 0,
        correlationId = testCorrelationId,
      )
      .unsafeRunSync()

    val _      = cRepo.findCallsRef.get() shouldBe List("hsxx99")
    val stored = aRepo.upsertCallsRef.get().headOption
    val _      = stored.flatMap(_.sponsorCommitteeId) shouldBe None
    // sponsorType still derives from the DTO's committee sponsor even though the FK is unresolved.
    stored.flatMap(_.sponsorType.map(_.toString)) shouldBe Some("Committee")
  }

  it should "leave sponsorCommitteeId None and not query the repo when the committee URL is unparseable" in {
    val d           = detail(committeeSponsorUrl = Some("https://api.congress.gov/v3/garbage"))
    val (api, _)    = mockApi(Map("117-SAMDT-100" -> d))
    val aRepo       = new StubAmendmentRepo(Map.empty)
    val bRepo       = new StubBillRepo
    val mRepo       = new StubMemberRepo
    val cRepo       = new StubCommitteeLookupRepo
    val placeholder = new StubPlaceholderCreator
    val processor   = buildProcessor(api, aRepo, bRepo, mRepo, placeholder, committeeRepo = cRepo)

    val _ = processor
      .processAmendment(
        naturalKey = "117-SAMDT-100",
        listItemOpt = Some(listItem()),
        detailUrlOpt = None,
        storedOpt = None,
        depth = 0,
        correlationId = testCorrelationId,
      )
      .unsafeRunSync()

    val _ = cRepo.findCallsRef.get() shouldBe Nil
    aRepo.upsertCallsRef.get().headOption.flatMap(_.sponsorCommitteeId) shouldBe None
  }

  it should "not query the committee repo when the sponsor is a member" in {
    val d           = detail(sponsorBioguide = Some("S001217"))
    val (api, _)    = mockApi(Map("117-SAMDT-100" -> d))
    val aRepo       = new StubAmendmentRepo(Map.empty)
    val bRepo       = new StubBillRepo
    val mRepo       = new StubMemberRepo
    val cRepo       = new StubCommitteeLookupRepo
    val placeholder = new StubPlaceholderCreator
    val processor   = buildProcessor(api, aRepo, bRepo, mRepo, placeholder, committeeRepo = cRepo)

    val _ = processor
      .processAmendment(
        naturalKey = "117-SAMDT-100",
        listItemOpt = Some(listItem()),
        detailUrlOpt = None,
        storedOpt = None,
        depth = 0,
        correlationId = testCorrelationId,
      )
      .unsafeRunSync()

    cRepo.findCallsRef.get() shouldBe Nil
  }

  it should "call BillRepository.upsertPlaceholder + findByBillId when DTO references a bill" in {
    val amended = AmendedBillDTO(
      congress = Some(117),
      number = Some("3684"),
      originChamber = None,
      originChamberCode = None,
      title = None,
      billType = Some("hr"),
      url = None,
      updateDateIncludingText = None,
    )
    val d           = detail(amendedBill = Some(amended))
    val (api, _)    = mockApi(Map("117-SAMDT-100" -> d))
    val aRepo       = new StubAmendmentRepo(Map.empty)
    val bRepo       = new StubBillRepo
    val mRepo       = new StubMemberRepo
    val placeholder = new StubPlaceholderCreator
    val processor   = buildProcessor(api, aRepo, bRepo, mRepo, placeholder)

    val _ = processor
      .processAmendment(
        naturalKey = "117-SAMDT-100",
        listItemOpt = Some(listItem()),
        detailUrlOpt = None,
        storedOpt = None,
        depth = 0,
        correlationId = testCorrelationId,
      )
      .unsafeRunSync()

    val _ = bRepo.placeholderCallsRef.get() shouldBe List("117-HR-3684")
    aRepo.upsertCallsRef.get().headOption.flatMap(_.billId) shouldBe Some(math.abs("117-HR-3684".hashCode.toLong))
  }

  it should "skip BillRepository.upsertPlaceholder when DTO has no amendedBill" in {
    val d           = detail(amendedBill = None)
    val (api, _)    = mockApi(Map("117-SAMDT-100" -> d))
    val aRepo       = new StubAmendmentRepo(Map.empty)
    val bRepo       = new StubBillRepo
    val mRepo       = new StubMemberRepo
    val placeholder = new StubPlaceholderCreator
    val processor   = buildProcessor(api, aRepo, bRepo, mRepo, placeholder)

    val _ = processor
      .processAmendment(
        naturalKey = "117-SAMDT-100",
        listItemOpt = Some(listItem()),
        detailUrlOpt = None,
        storedOpt = None,
        depth = 0,
        correlationId = testCorrelationId,
      )
      .unsafeRunSync()

    bRepo.placeholderCallsRef.get() shouldBe Nil
  }

  it should "thread the SAME correlationId through every recursion frame (parent fetch + child fetch)" in {
    val parentRef = AmendedAmendmentDTO(
      congress = Some(117),
      number = Some("90"),
      amendmentType = Some("samdt"),
      purpose = None,
      updateDate = Some("2024-05-30T00:00:00Z"),
      url = None,
    )
    val childDetail  = detail(number = "100", amendedAmendment = Some(parentRef))
    val parentDetail = detail(number = "90", amendedAmendment = None, amendedBill = None)
    val (api, calls) =
      mockApi(Map("117-SAMDT-100" -> childDetail, "117-SAMDT-90" -> parentDetail))
    val aRepo       = new StubAmendmentRepo(Map.empty)
    val bRepo       = new StubBillRepo
    val mRepo       = new StubMemberRepo
    val placeholder = new StubPlaceholderCreator
    val processor   = buildProcessor(api, aRepo, bRepo, mRepo, placeholder)

    val _ = processor
      .processAmendment(
        naturalKey = "117-SAMDT-100",
        listItemOpt = Some(listItem()),
        detailUrlOpt = None,
        storedOpt = None,
        depth = 0,
        correlationId = testCorrelationId,
      )
      .unsafeRunSync()

    val ids = calls.get().map(_.correlationId).distinct
    val _   = ids shouldBe List(testCorrelationId)
    // Child fetch uses the listItem-supplied URL; parent recursion has no URL on its `amendedAmendment`
    // ref, so the processor falls back to the natural key. The TestApiClient's stub map treats the URL
    // last-3-segments (or the natural key directly) as a lookup, so both forms hit the right detail.
    calls.get().map(_.detailUrl).toSet shouldBe Set(
      "https://api.congress.gov/v3/amendment/117/samdt/100",
      "117-SAMDT-90",
    )
  }

  it should "short-circuit parent recursion when the parent is already hydrated (updateDate.isDefined)" in {
    val parentRef = AmendedAmendmentDTO(
      congress = Some(117),
      number = Some("90"),
      amendmentType = Some("samdt"),
      purpose = None,
      updateDate = Some("2024-05-30T00:00:00Z"),
      url = None,
    )
    val childDetail  = detail(number = "100", amendedAmendment = Some(parentRef))
    val (api, calls) = mockApi(Map("117-SAMDT-100" -> childDetail))
    val parentStored = storedAmendment(
      naturalKey = "117-SAMDT-90",
      updateDate = Some(Instant.parse("2024-05-30T00:00:00Z")),
      billId = Some(42L),
    )
    val aRepo       = new StubAmendmentRepo(Map("117-SAMDT-90" -> parentStored))
    val bRepo       = new StubBillRepo
    val mRepo       = new StubMemberRepo
    val placeholder = new StubPlaceholderCreator
    val processor   = buildProcessor(api, aRepo, bRepo, mRepo, placeholder)

    val _ = processor
      .processAmendment(
        naturalKey = "117-SAMDT-100",
        listItemOpt = Some(listItem()),
        detailUrlOpt = None,
        storedOpt = None,
        depth = 0,
        correlationId = testCorrelationId,
      )
      .unsafeRunSync()

    // Only the child detail should have been fetched; parent is already hydrated.
    val _ = calls.get().map(_.detailUrl) shouldBe List(
      "https://api.congress.gov/v3/amendment/117/samdt/100"
    )
    aRepo.upsertCallsRef.get().headOption match {
      case Some(saved) =>
        val _ = saved.parentAmendmentId shouldBe Some(parentStored.amendmentId)
        saved.billId shouldBe Some(42L) // inherited from parent
      case None => fail("expected an upsert")
    }
  }

  it should "compute effectiveBillId = directBillId.orElse(parentBillId)" in {
    val parentRef = AmendedAmendmentDTO(
      congress = Some(117),
      number = Some("90"),
      amendmentType = Some("samdt"),
      purpose = None,
      updateDate = Some("2024-05-30T00:00:00Z"),
      url = None,
    )
    val directBill = AmendedBillDTO(
      congress = Some(117),
      number = Some("3684"),
      originChamber = None,
      originChamberCode = None,
      title = None,
      billType = Some("hr"),
      url = None,
      updateDateIncludingText = None,
    )
    val childDetail = detail(
      number = "100",
      amendedAmendment = Some(parentRef),
      amendedBill = Some(directBill),
    )
    val (api, _) = mockApi(Map("117-SAMDT-100" -> childDetail))
    val parentStored =
      storedAmendment(
        naturalKey = "117-SAMDT-90",
        updateDate = Some(Instant.parse("2024-05-30T00:00:00Z")),
        billId = Some(99L),
      )
    val aRepo       = new StubAmendmentRepo(Map("117-SAMDT-90" -> parentStored))
    val bRepo       = new StubBillRepo
    val mRepo       = new StubMemberRepo
    val placeholder = new StubPlaceholderCreator
    val processor   = buildProcessor(api, aRepo, bRepo, mRepo, placeholder)

    val _ = processor
      .processAmendment(
        naturalKey = "117-SAMDT-100",
        listItemOpt = Some(listItem()),
        detailUrlOpt = None,
        storedOpt = None,
        depth = 0,
        correlationId = testCorrelationId,
      )
      .unsafeRunSync()

    val expected = math.abs("117-HR-3684".hashCode.toLong)
    aRepo.upsertCallsRef.get().headOption.flatMap(_.billId) shouldBe Some(expected)
  }

  it should "produce ProcessingResult.Failed when toDO returns Left" in {
    val badDetail   = detail(number = "")
    val (api, _)    = mockApi(Map("117-SAMDT-" -> badDetail))
    val aRepo       = new StubAmendmentRepo(Map.empty)
    val bRepo       = new StubBillRepo
    val mRepo       = new StubMemberRepo
    val placeholder = new StubPlaceholderCreator
    val processor   = buildProcessor(api, aRepo, bRepo, mRepo, placeholder)

    val result = processor
      .processAmendment(
        naturalKey = "117-SAMDT-",
        listItemOpt = Some(listItem(number = "")),
        detailUrlOpt = None,
        storedOpt = None,
        depth = 0,
        correlationId = testCorrelationId,
      )
      .unsafeRunSync()

    result match {
      case f: ProcessingResult.Failed =>
        val _ = f.reason should include("DTO-to-DO conversion failed")
        aRepo.upsertCallsRef.get() shouldBe Nil
      case other => fail(s"expected Failed, got $other")
    }
  }

  it should "increment orphanResolved when the recursion bottoms out without a bill" in {
    val d           = detail(amendedBill = None, amendedAmendment = None)
    val (api, _)    = mockApi(Map("117-SAMDT-100" -> d))
    val aRepo       = new StubAmendmentRepo(Map.empty)
    val bRepo       = new StubBillRepo
    val mRepo       = new StubMemberRepo
    val placeholder = new StubPlaceholderCreator
    val metrics     = AmendmentMetrics.make()
    val processor   = buildProcessor(api, aRepo, bRepo, mRepo, placeholder, metrics = metrics)

    val _ = processor
      .processAmendment(
        naturalKey = "117-SAMDT-100",
        listItemOpt = Some(listItem()),
        detailUrlOpt = None,
        storedOpt = None,
        depth = 0,
        correlationId = testCorrelationId,
      )
      .unsafeRunSync()

    metrics.snapshot().orphanResolved shouldBe 1L
  }

  // --------------------------------------------------------------------------------------------
  // streamAll — page-batch SELECT + stream-level wiring
  // --------------------------------------------------------------------------------------------

  "streamAll" should "issue exactly ONE findByNaturalKeys per page (P2 batch optimization)" in {
    val items = List(
      listItem(number = "100", updateDate = Some("2024-06-02T12:00:00Z")),
      listItem(number = "101", updateDate = Some("2024-06-02T12:00:00Z")),
      listItem(number = "102", updateDate = Some("2024-06-02T12:00:00Z")),
    )
    val details = items
      .map(it => AmendmentNaturalKeys.fromListItem(it) -> detail(number = it.number, amendedBill = None))
      .toMap

    val (api, _)    = mockApi(detailMap = details, pageItems = items)
    val aRepo       = new StubAmendmentRepo(Map.empty)
    val bRepo       = new StubBillRepo
    val mRepo       = new StubMemberRepo
    val placeholder = new StubPlaceholderCreator
    val processor =
      buildProcessor(api, aRepo, bRepo, mRepo, placeholder, config = baseConfig.copy(parallelism = 1))

    val results = processor.streamAll("run-1").compile.toList.unsafeRunSync()

    val _          = results.size shouldBe 3
    val batchCalls = aRepo.findByNaturalKeysCallsRef.get()
    val _          = batchCalls.size shouldBe 1
    batchCalls.headOption.map(_.toSet) shouldBe Some(items.map(AmendmentNaturalKeys.fromListItem).toSet)
  }

  it should "not abort the run when list pagination fails (e.g. exhausted 429 retries)" in {
    val failingCalls = new AtomicReference[List[ApiCall]](Nil)
    val failingApi = new TestApiClient(Map.empty, Nil, failingCalls) {
      override def fetchPage(params: FetchParams): IO[PagedResponse[AmendmentListItemDTO]] =
        IO.raiseError[PagedResponse[AmendmentListItemDTO]](new RuntimeException("429 exhausted"))
    }
    val processor =
      buildProcessor(
        failingApi,
        new StubAmendmentRepo(Map.empty),
        new StubBillRepo,
        new StubMemberRepo,
        new StubPlaceholderCreator,
      )

    // The stream-level handler must swallow the list-pagination failure: the run completes (empty),
    // it does NOT raise — so the next idempotent re-run can resume instead of every run crashing.
    processor.streamAll("run-rate-limit").compile.toList.unsafeRunSync() shouldBe empty
  }

  it should "process partially-stored pages — items not in DB are processed as new, items unchanged are skipped" in {
    val items = List(
      listItem(number = "100", updateDate = Some("2024-06-02T12:00:00Z")),
      listItem(number = "101", updateDate = Some("2024-06-02T12:00:00Z")),
    )
    val details = items
      .map(it => AmendmentNaturalKeys.fromListItem(it) -> detail(number = it.number, amendedBill = None))
      .toMap

    val (api, calls) = mockApi(detailMap = details, pageItems = items)
    val pre = storedAmendment(
      naturalKey = "117-SAMDT-100",
      updateDate = Some(Instant.parse("2024-06-02T12:00:00Z")),
    )
    val aRepo       = new StubAmendmentRepo(Map(pre.naturalKey -> pre))
    val bRepo       = new StubBillRepo
    val mRepo       = new StubMemberRepo
    val placeholder = new StubPlaceholderCreator
    val processor =
      buildProcessor(api, aRepo, bRepo, mRepo, placeholder, config = baseConfig.copy(parallelism = 1))

    val results = processor.streamAll("run-2").compile.toList.unsafeRunSync()

    val _ = results.count(_.isSkipped) shouldBe 1
    val _ = results.count(_.isSucceeded) shouldBe 1
    calls.get().map(_.detailUrl) shouldBe List(
      "https://api.congress.gov/v3/amendment/117/samdt/101"
    )
  }

  it should "filter out items whose congress falls outside [congressesMin, congressesMax] and bump the metric" in {
    val items = List(
      listItem(congress = 101, number = "10", updateDate = Some("2024-06-02T12:00:00Z")), // below 102 cutoff
      listItem(congress = 117, number = "11", updateDate = Some("2024-06-02T12:00:00Z")), // in-range
      listItem(congress = 130, number = "12", updateDate = Some("2024-06-02T12:00:00Z")), // above max
    )
    val details = items
      .filter(it => it.congress >= 117 && it.congress <= 117)
      .map(it => AmendmentNaturalKeys.fromListItem(it) -> detail(congress = it.congress, number = it.number))
      .toMap

    val (api, calls) = mockApi(detailMap = details, pageItems = items)
    val aRepo        = new StubAmendmentRepo(Map.empty)
    val bRepo        = new StubBillRepo
    val mRepo        = new StubMemberRepo
    val placeholder  = new StubPlaceholderCreator
    val metrics      = AmendmentMetrics.make()
    val processor =
      buildProcessor(
        api,
        aRepo,
        bRepo,
        mRepo,
        placeholder,
        config = baseConfig.copy(congressesMin = 117, congressesMax = 117, parallelism = 1),
        metrics = metrics,
      )

    val results = processor.streamAll("run-filter").compile.toList.unsafeRunSync()

    val _ = results.size shouldBe 1
    val _ = results.headOption.map(_.entityId) shouldBe Some("117-SAMDT-11")
    val _ = calls.get().map(_.detailUrl) shouldBe List(
      "https://api.congress.gov/v3/amendment/117/samdt/11"
    )
    val _ = aRepo.upsertCallsRef.get().size shouldBe 1
    metrics.snapshot().congressOutOfRange shouldBe 2L
  }

  it should "issue findByNaturalKeys with only in-range keys when out-of-range items exist" in {
    val items = List(
      listItem(congress = 101, number = "10", updateDate = Some("2024-06-02T12:00:00Z")), // below cutoff
      listItem(congress = 117, number = "11", updateDate = Some("2024-06-02T12:00:00Z")),
    )
    val details = Map(
      "117-SAMDT-11" -> detail(congress = 117, number = "11")
    )

    val (api, _)    = mockApi(detailMap = details, pageItems = items)
    val aRepo       = new StubAmendmentRepo(Map.empty)
    val bRepo       = new StubBillRepo
    val mRepo       = new StubMemberRepo
    val placeholder = new StubPlaceholderCreator
    val metrics     = AmendmentMetrics.make()
    val processor =
      buildProcessor(
        api,
        aRepo,
        bRepo,
        mRepo,
        placeholder,
        config = baseConfig.copy(congressesMin = 117, congressesMax = 117, parallelism = 1),
        metrics = metrics,
      )

    val _ = processor.streamAll("run-filter-batch").compile.toList.unsafeRunSync()

    val batchCalls = aRepo.findByNaturalKeysCallsRef.get()
    val _          = batchCalls.size shouldBe 1
    batchCalls.headOption shouldBe Some(List("117-SAMDT-11"))
  }

  it should "emit a unique correlationId per LIST-page amendment" in {
    val items = List(
      listItem(number = "100", updateDate = Some("2024-06-02T12:00:00Z")),
      listItem(number = "101", updateDate = Some("2024-06-02T12:00:00Z")),
      listItem(number = "102", updateDate = Some("2024-06-02T12:00:00Z")),
    )
    val details = items
      .map(it => AmendmentNaturalKeys.fromListItem(it) -> detail(number = it.number, amendedBill = None))
      .toMap

    val (api, calls) = mockApi(detailMap = details, pageItems = items)
    val aRepo        = new StubAmendmentRepo(Map.empty)
    val bRepo        = new StubBillRepo
    val mRepo        = new StubMemberRepo
    val placeholder  = new StubPlaceholderCreator
    val processor =
      buildProcessor(api, aRepo, bRepo, mRepo, placeholder, config = baseConfig.copy(parallelism = 1))

    val _ = processor.streamAll("run-3").compile.toList.unsafeRunSync()

    val ids = calls.get().map(_.correlationId)
    ids.distinct.size shouldBe ids.size
  }

  // --------------------------------------------------------------------------------------------
  // summarize
  // --------------------------------------------------------------------------------------------

  "summarize" should "compute counts and force eventsEmitted = 0" in {
    val (api, _)    = mockApi()
    val aRepo       = new StubAmendmentRepo(Map.empty)
    val bRepo       = new StubBillRepo
    val mRepo       = new StubMemberRepo
    val placeholder = new StubPlaceholderCreator
    val processor   = buildProcessor(api, aRepo, bRepo, mRepo, placeholder)

    val results: List[ProcessingResult] = List(
      ProcessingResult.Succeeded("a-1"),
      ProcessingResult.Succeeded("a-2"),
      ProcessingResult.Succeeded("a-3"),
      ProcessingResult.Skipped("a-4", "unchanged"),
      ProcessingResult.Skipped("a-5", "unchanged"),
      ProcessingResult.Failed("a-6", "boom"),
    )

    val summary = processor.summarize(results)
    val _       = summary.totalProcessed shouldBe 6
    val _       = summary.succeeded shouldBe 3
    val _       = summary.skipped shouldBe 2
    val _       = summary.failed shouldBe 1
    val _       = summary.eventsEmitted shouldBe 0
    summary.errors shouldBe List("boom")
  }

  // --------------------------------------------------------------------------------------------
  // Recursion — depth-bound integration
  // --------------------------------------------------------------------------------------------

  it should "trip the depth guard when a fixture chain exceeds maxRecursionDepth" in {
    val urls = (96 to 100).toList.reverse
    val details = urls.zipWithIndex.map {
      case (n, idx) =>
        val nextN = if (idx == urls.size - 1) { None }
        else { Some(urls(idx + 1)) }
        val parentRef = nextN.map(p =>
          AmendedAmendmentDTO(
            congress = Some(117),
            number = Some(p.toString),
            amendmentType = Some("samdt"),
            purpose = None,
            updateDate = Some("2024-05-30T00:00:00Z"),
            url = None,
          )
        )
        s"117-SAMDT-${n.toString}" -> detail(number = n.toString, amendedAmendment = parentRef)
    }.toMap

    val (api, _)    = mockApi(detailMap = details)
    val aRepo       = new StubAmendmentRepo(Map.empty)
    val bRepo       = new StubBillRepo
    val mRepo       = new StubMemberRepo
    val placeholder = new StubPlaceholderCreator
    val metrics     = AmendmentMetrics.make()
    val processor =
      buildProcessor(
        api,
        aRepo,
        bRepo,
        mRepo,
        placeholder,
        config = baseConfig.copy(maxRecursionDepth = 3),
        metrics = metrics,
      )

    val _ = processor
      .processAmendment(
        naturalKey = "117-SAMDT-100",
        listItemOpt = Some(listItem(number = "100")),
        detailUrlOpt = None,
        storedOpt = None,
        depth = 0,
        correlationId = testCorrelationId,
      )
      .unsafeRunSync()

    metrics.snapshot().recursionDepthExceeded should be >= 1L
  }

}
