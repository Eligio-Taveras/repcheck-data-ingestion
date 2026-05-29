package repcheck.members.committees.pipeline

import cats.effect.IO
import cats.effect.unsafe.implicits.global

import doobie._
import doobie.free.connection

import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.{any, anyInt}
import org.mockito.Mockito.{times, verify, when}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar
import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}
import repcheck.members.committees.client.CdirCommitteeSource
import repcheck.members.committees.config.HistoricalLoaderConfig
import repcheck.members.committees.errors.CommitteeMemberUpsertFailed
import repcheck.members.committees.model.{
  CommitteeDO,
  CommitteeMemberInsert,
  HistoricalLoadResult,
  HistoricalMemberRow,
  UsStateNames,
}
import repcheck.members.committees.persistence.{
  CommitteeMemberRepository,
  CommitteeRepository,
  HistoricalMemberRepository,
}

class CommitteeHistoryLoaderSpec extends AnyFlatSpec with Matchers with MockitoSugar {

  private val testXa: Transactor[IO] = Transactor.fromDriverManager[IO](
    driver = "org.h2.Driver",
    url = "jdbc:h2:mem:committee-history;DB_CLOSE_DELAY=-1",
    user = "",
    password = "",
    logHandler = None,
  )

  private val config = HistoricalLoaderConfig(currentCongress = 117, lookbackCongresses = 1)

  private val noopLogger: PipelineLogger[IO] = new PipelineLogger[IO] {
    def info(context: LogContext, message: String): IO[Unit]                            = IO.unit
    def warn(context: LogContext, message: String): IO[Unit]                            = IO.unit
    def error(context: LogContext, message: String, cause: Option[Throwable]): IO[Unit] = IO.unit
    def debug(context: LogContext, message: String): IO[Unit]                           = IO.unit
  }

  private val agricultureText: String =
    """STANDING COMMITTEES OF THE HOUSE
      |Agriculture
      |1301 Longworth House Office Building, phone 225-2171
      |David Scott, of Georgia, Chair
      |Jim Costa, of California.            Glenn Thompson, of Pennsylvania.""".stripMargin

  private val subcommitteeText: String =
    """STANDING COMMITTEES OF THE HOUSE
      |Agriculture
      |David Scott, of Georgia, Chair
      |Subcommittee on Livestock
      |Jim Costa, of California.""".stripMargin

  private def sourceReturning(texts: List[String]): CdirCommitteeSource[IO] =
    new CdirCommitteeSource[IO] {
      def committeeListingTexts(congress: Int, runId: Long): IO[List[String]] = IO.pure(texts)
    }

  private def committee(id: Long, name: String): CommitteeDO =
    CommitteeDO(id, "AG00", name, "House", Some("Standing"), None, None, None, Some(true), None, None)

  private def loader(
    source: CdirCommitteeSource[IO],
    committeeRepo: CommitteeRepository,
    committeeMemberRepo: CommitteeMemberRepository,
    memberRepo: HistoricalMemberRepository,
  ): CommitteeHistoryLoader[IO] =
    new CommitteeHistoryLoader[IO](
      source,
      committeeRepo,
      committeeMemberRepo,
      memberRepo,
      testXa,
      config,
      UsStateNames.all,
      noopLogger,
    )

  private def mocks(
    members: List[HistoricalMemberRow],
    committees: List[CommitteeDO] = List(committee(7L, "Agriculture Committee")),
  ): (CommitteeRepository, CommitteeMemberRepository, HistoricalMemberRepository) = {
    val committeeRepo       = mock[CommitteeRepository]
    val committeeMemberRepo = mock[CommitteeMemberRepository]
    val memberRepo          = mock[HistoricalMemberRepository]
    val _                   = when(committeeRepo.listAll()).thenReturn(connection.pure(committees))
    val _                   = when(memberRepo.membersForCongress(anyInt())).thenReturn(connection.pure(members))
    val _ = when(committeeMemberRepo.upsert(any[CommitteeMemberInsert])).thenReturn(connection.pure(()))
    (committeeRepo, committeeMemberRepo, memberRepo)
  }

  "load" should "disambiguate same-surname members by first name and upsert under the target congress" in {
    val members = List(
      HistoricalMemberRow(Some("David"), Some("Scott"), Some("Georgia"), 42L),
      HistoricalMemberRow(Some("Austin"), Some("Scott"), Some("Georgia"), 44L), // same (last,state)
      HistoricalMemberRow(Some("Jim"), Some("Costa"), Some("California"), 43L),
    )
    val (cr, cmr, mr) = mocks(members)
    val result        = loader(sourceReturning(List(agricultureText)), cr, cmr, mr).load(1L).unsafeRunSync()

    val _      = result.upserted shouldBe 2        // David Scott (disambiguated), Jim Costa
    val _      = result.skippedNoMember shouldBe 1 // Glenn Thompson not in member set
    val captor = ArgumentCaptor.forClass(classOf[CommitteeMemberInsert])
    val _      = verify(cmr, times(2)).upsert(captor.capture())
    val _      = captor.getAllValues.stream.allMatch(_.congress == 117) shouldBe true
    captor.getAllValues.stream.anyMatch(_.memberId == 42L) shouldBe true
  }

  it should "count subcommittee assignments whose committee is not in the DB as skippedNoCommittee" in {
    val members = List(
      HistoricalMemberRow(Some("Jim"), Some("Costa"), Some("California"), 43L),
      HistoricalMemberRow(Some("David"), Some("Scott"), Some("Georgia"), 42L),
    )
    val (cr, cmr, mr) = mocks(members)
    val result        = loader(sourceReturning(List(subcommitteeText)), cr, cmr, mr).load(1L).unsafeRunSync()

    val _ = result.upserted shouldBe 1 // David Scott on Agriculture
    result.skippedNoCommittee shouldBe 1 // Jim Costa on "Subcommittee on Livestock" (no DB row)
  }

  it should "raise CommitteeMemberUpsertFailed when the upsert fails" in {
    val members       = List(HistoricalMemberRow(Some("David"), Some("Scott"), Some("Georgia"), 42L))
    val (cr, cmr, mr) = mocks(members)
    val _ = when(cmr.upsert(any[CommitteeMemberInsert]))
      .thenReturn(connection.raiseError(new RuntimeException("boom")))

    assertThrows[CommitteeMemberUpsertFailed] {
      loader(sourceReturning(List(agricultureText)), cr, cmr, mr).load(1L).unsafeRunSync()
    }
  }

  it should "return empty when the source yields no text" in {
    val (cr, cmr, mr) = mocks(Nil)
    loader(sourceReturning(Nil), cr, cmr, mr).load(1L).unsafeRunSync() shouldBe HistoricalLoadResult.empty
  }

}
