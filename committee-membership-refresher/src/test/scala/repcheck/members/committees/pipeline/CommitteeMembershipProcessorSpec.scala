package repcheck.members.committees.pipeline

import java.time.Instant

import scala.concurrent.duration._

import cats.effect.IO
import cats.effect.unsafe.implicits.global

import doobie._
import doobie.free.connection

import org.mockito.ArgumentMatchers.{any, anyLong, anyString, eq => eqTo}
import org.mockito.Mockito.{never, times, verify, when}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar
import repcheck.ingestion.common.api.{FetchParams, PagedResponse}
import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}
import repcheck.members.committees.client.{
  CommitteeMetadataApiClient,
  HouseMemberDataXmlClient,
  SenateCommitteeMembershipXmlClient,
  SenateIdentityXmlClient,
}
import repcheck.members.committees.config.CommitteeMembershipConfig
import repcheck.members.committees.model._
import repcheck.members.committees.persistence.{CommitteeMemberRepository, CommitteeRepository}
import repcheck.members.common.persistence.MemberRepository
import repcheck.shared.models.congress.common.{Party, UsState}
import repcheck.shared.models.congress.dos.member.MemberDO

class CommitteeMembershipProcessorSpec extends AnyFlatSpec with Matchers with MockitoSugar {

  private val testXa: Transactor[IO] = Transactor.fromDriverManager[IO](
    driver = "org.h2.Driver",
    url = "jdbc:h2:mem:committee;DB_CLOSE_DELAY=-1",
    user = "",
    password = "",
    logHandler = None,
  )

  private val runId = 12345L

  private val config = CommitteeMembershipConfig(
    parallelism = 1,
    requestTimeout = 5.seconds,
    currentCongress = 119,
    houseMemberDataUrl = "http://localhost/house.xml",
    senateIdentityUrl = "http://localhost/identity.xml",
    senateCommitteeBaseUrl = "http://localhost/senate",
  )

  private case class TestFixture(
    committeeApiClient: CommitteeMetadataApiClient[IO],
    houseXmlClient: HouseMemberDataXmlClient[IO],
    senateIdentityClient: SenateIdentityXmlClient[IO],
    senateCommitteeClient: SenateCommitteeMembershipXmlClient[IO],
    committeeRepo: CommitteeRepository,
    committeeMemberRepo: CommitteeMemberRepository,
    memberRepo: MemberRepository,
    logger: PipelineLogger[IO],
  ) {

    def processor: CommitteeMembershipProcessor[IO] =
      new CommitteeMembershipProcessor[IO](
        committeeApiClient = committeeApiClient,
        houseXmlClient = houseXmlClient,
        senateIdentityClient = senateIdentityClient,
        senateCommitteeClient = senateCommitteeClient,
        committeeRepo = committeeRepo,
        committeeMemberRepo = committeeMemberRepo,
        memberRepo = memberRepo,
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
      committeeApiClient = mock[CommitteeMetadataApiClient[IO]],
      houseXmlClient = mock[HouseMemberDataXmlClient[IO]],
      senateIdentityClient = mock[SenateIdentityXmlClient[IO]],
      senateCommitteeClient = mock[SenateCommitteeMembershipXmlClient[IO]],
      committeeRepo = mock[CommitteeRepository],
      committeeMemberRepo = mock[CommitteeMemberRepository],
      memberRepo = mock[MemberRepository],
      logger = loggerMock,
    )
  }

  private def makeMember(
    memberId: Long,
    bioguideId: String,
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

  private def stubPhase1Empty(f: TestFixture): Unit = {
    val _ = when(f.committeeApiClient.fetchPageForChamber(anyString(), any[FetchParams]))
      .thenReturn(IO.pure(PagedResponse(items = List.empty, totalCount = 0, nextOffset = None)))
  }

  private def stubPhase2Empty(f: TestFixture): Unit = {
    val _ = when(f.houseXmlClient.fetchMembers(anyLong())).thenReturn(fs2.Stream.empty)
  }

  private def stubPhase3Empty(f: TestFixture): Unit = {
    val _ = when(f.senateIdentityClient.fetchIdentities(anyLong())).thenReturn(fs2.Stream.empty)
    val _ = when(f.committeeRepo.findAllSenateParentCodes()).thenReturn(connection.pure(List.empty[String]))
  }

  "refreshAll" should "return zero counts when all phases produce empty results" in {
    val f = createFixture()
    stubPhase1Empty(f)
    stubPhase2Empty(f)
    stubPhase3Empty(f)

    val result = f.processor.refreshAll(runId).unsafeRunSync()

    val _ = result shouldBe CommitteeMembershipRefreshResult.empty
    verify(f.committeeApiClient, times(3)).fetchPageForChamber(anyString(), any[FetchParams])
  }

  it should "process House members and create committee memberships" in {
    val f = createFixture()
    stubPhase1Empty(f)
    stubPhase3Empty(f)

    val houseDto = HouseMemberDataXmlDTO(
      bioguideId = "H001",
      firstName = "John",
      lastName = "Doe",
      party = "R",
      state = "Texas",
      district = Some(1),
      committees = List(
        HouseCommitteeAssignmentXmlDTO("RU00", "Rules", Some(1), Some("majority")),
        HouseCommitteeAssignmentXmlDTO("AP00", "Appropriations", Some(5), None),
      ),
    )

    val _ = when(f.houseXmlClient.fetchMembers(anyLong())).thenReturn(fs2.Stream.emit(houseDto))
    val _ = when(f.memberRepo.findByBioguideId(eqTo("H001")))
      .thenReturn(connection.pure(Some(makeMember(memberId = 100L, bioguideId = "H001"))))
    val _ = when(f.committeeRepo.upsertPlaceholder(anyString(), anyString()))
      .thenReturn(connection.pure(()))
    val _ = when(f.committeeMemberRepo.upsert(any[CommitteeMemberDO]))
      .thenReturn(connection.pure(()))

    val result = f.processor.refreshAll(runId).unsafeRunSync()

    val _ = result.houseMembersProcessed shouldBe 1
    val _ = result.membershipRowsUpserted shouldBe 2
    verify(f.committeeMemberRepo, times(2)).upsert(any[CommitteeMemberDO])
  }

  it should "skip House members not in the members table" in {
    val f = createFixture()
    stubPhase1Empty(f)
    stubPhase3Empty(f)

    val houseDto = HouseMemberDataXmlDTO(
      bioguideId = "UNKNOWN",
      firstName = "Ghost",
      lastName = "Member",
      party = "D",
      state = "Ohio",
      district = None,
      committees = List(HouseCommitteeAssignmentXmlDTO("RU00", "Rules", None, None)),
    )

    val _ = when(f.houseXmlClient.fetchMembers(anyLong())).thenReturn(fs2.Stream.emit(houseDto))
    val _ = when(f.memberRepo.findByBioguideId(eqTo("UNKNOWN")))
      .thenReturn(connection.pure(Option.empty[MemberDO]))

    val result = f.processor.refreshAll(runId).unsafeRunSync()

    val _ = result.houseMembersProcessed shouldBe 0
    val _ = result.membershipRowsUpserted shouldBe 0
    verify(f.committeeMemberRepo, never()).upsert(any[CommitteeMemberDO])
  }

  it should "propagate XML client failure" in {
    val f = createFixture()
    stubPhase1Empty(f)
    stubPhase3Empty(f)

    val _ = when(f.houseXmlClient.fetchMembers(anyLong()))
      .thenReturn(fs2.Stream.raiseError[IO](new RuntimeException("House XML unreachable")))

    val thrown = intercept[RuntimeException] {
      val _ = f.processor.refreshAll(runId).unsafeRunSync()
    }
    thrown.getMessage shouldBe "House XML unreachable"
  }

  it should "upsert committees from Congress.gov API in phase 1" in {
    val f = createFixture()
    stubPhase2Empty(f)
    stubPhase3Empty(f)

    val committeeItem = CommitteeListItemDTO(
      systemCode = "hsru00",
      name = "Committee on Rules",
      chamber = "House",
      committeeTypeCode = Some("Standing"),
      updateDate = Some("2024-01-01"),
      url = None,
      parent = None,
      subcommittees = None,
    )

    val _ = when(f.committeeApiClient.fetchPageForChamber(eqTo("house"), any[FetchParams]))
      .thenReturn(IO.pure(PagedResponse(items = List(committeeItem), totalCount = 1, nextOffset = None)))
    val _ = when(f.committeeApiClient.fetchPageForChamber(eqTo("senate"), any[FetchParams]))
      .thenReturn(IO.pure(PagedResponse(items = List.empty, totalCount = 0, nextOffset = None)))
    val _ = when(f.committeeApiClient.fetchPageForChamber(eqTo("joint"), any[FetchParams]))
      .thenReturn(IO.pure(PagedResponse(items = List.empty, totalCount = 0, nextOffset = None)))
    val _ = when(f.committeeRepo.upsert(any[CommitteeDO]))
      .thenReturn(connection.pure(()))

    val result = f.processor.refreshAll(runId).unsafeRunSync()

    val _ = result.committeesUpserted shouldBe 1
    verify(f.committeeRepo, times(1)).upsert(any[CommitteeDO])
  }

}
