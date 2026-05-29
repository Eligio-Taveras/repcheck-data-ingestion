package repcheck.members.committees.pipeline

import java.time.Instant

import cats.effect.IO
import cats.effect.unsafe.implicits.global

import fs2.Stream

import doobie._
import doobie.free.connection

import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.{any, anyString, eq => eqTo}
import org.mockito.Mockito.{never, verify, when}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar
import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}
import repcheck.members.committees.client.HistoricalAssignmentTsvReader
import repcheck.members.committees.config.HistoricalLoaderConfig
import repcheck.members.committees.model.{CommitteeDO, CommitteeInsert, CommitteeMemberInsert}
import repcheck.members.committees.persistence.{CommitteeMemberRepository, CommitteeRepository}
import repcheck.members.common.persistence.MemberRepository
import repcheck.shared.models.congress.common.{Party, UsState}
import repcheck.shared.models.congress.dos.member.MemberDO

class CommitteeHistoryLoaderSpec extends AnyFlatSpec with Matchers with MockitoSugar {

  private val testXa: Transactor[IO] = Transactor.fromDriverManager[IO](
    driver = "org.h2.Driver",
    url = "jdbc:h2:mem:committee-history;DB_CLOSE_DELAY=-1",
    user = "",
    password = "",
    logHandler = None,
  )

  private val config = HistoricalLoaderConfig(filePath = "/unused.tsv", parallelism = 1)

  private val noopLogger: PipelineLogger[IO] = new PipelineLogger[IO] {
    def info(context: LogContext, message: String): IO[Unit]                            = IO.unit
    def warn(context: LogContext, message: String): IO[Unit]                            = IO.unit
    def error(context: LogContext, message: String, cause: Option[Throwable]): IO[Unit] = IO.unit
    def debug(context: LogContext, message: String): IO[Unit]                           = IO.unit
  }

  private def member(memberId: Long, bioguide: String): MemberDO =
    MemberDO(
      memberId = memberId,
      naturalKey = bioguide,
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

  private def committee(id: Long, code: String, chamber: String): CommitteeDO =
    CommitteeDO(id, code, code, chamber, Some("Standing"), None, None, None, Some(true), None, None)

  private case class Mocks(
    committeeRepo: CommitteeRepository,
    committeeMemberRepo: CommitteeMemberRepository,
    memberRepo: MemberRepository,
  ) {

    def loader: CommitteeHistoryLoader[IO] =
      new CommitteeHistoryLoader[IO](committeeRepo, committeeMemberRepo, memberRepo, testXa, config, noopLogger)

  }

  private def mocks(): Mocks = {
    val committeeRepo       = mock[CommitteeRepository]
    val committeeMemberRepo = mock[CommitteeMemberRepository]
    val memberRepo          = mock[MemberRepository]
    val _ = when(committeeMemberRepo.upsert(any[CommitteeMemberInsert])).thenReturn(connection.pure(()))
    Mocks(committeeRepo, committeeMemberRepo, memberRepo)
  }

  private val header = HistoricalAssignmentTsvReader.Header

  private def tsv(rows: String*): Stream[IO, String] = Stream.emits(header +: rows.toList)

  private def line(
    congress: Int = 117,
    chamber: String = "Senate",
    code: String = "SSJU00",
    name: String = "Judiciary Committee",
    cType: String = "Standing",
    bioguide: String = "B001",
    role: String = "Chairman",
    rank: String = "1",
  ): String =
    List(congress.toString, chamber, code, name, cType, bioguide, role, rank).mkString("\t")

  "load" should "upsert a membership under the row's own congress when member and committee exist" in {
    val m = mocks()
    val _ = when(m.memberRepo.findByBioguideId(eqTo("B001"))).thenReturn(connection.pure(Some(member(42L, "B001"))))
    val _ = when(m.committeeRepo.findByCode(eqTo("SSJU00")))
      .thenReturn(connection.pure(Some(committee(7L, "SSJU00", "Senate"))))

    val result = m.loader.load(tsv(line(congress = 114)), 1L).unsafeRunSync()

    val _      = result.rowsRead shouldBe 1
    val _      = result.upserted shouldBe 1
    val captor = ArgumentCaptor.forClass(classOf[CommitteeMemberInsert])
    val _      = verify(m.committeeMemberRepo).upsert(captor.capture())
    val insert = captor.getValue
    val _      = insert.congress shouldBe 114
    val _      = insert.committeeId shouldBe 7L
    val _      = insert.memberId shouldBe 42L
    insert.role shouldBe Some("Chairman")
  }

  it should "skip and count assignments whose member is not in the members table" in {
    val m = mocks()
    val _ = when(m.memberRepo.findByBioguideId(anyString())).thenReturn(connection.pure(None))

    val result = m.loader.load(tsv(line()), 1L).unsafeRunSync()

    val _ = result.skippedNoMember shouldBe 1
    val _ = result.upserted shouldBe 0
    verify(m.committeeMemberRepo, never).upsert(any[CommitteeMemberInsert])
  }

  it should "count malformed rows as parse errors without aborting" in {
    val m = mocks()
    val _ = when(m.memberRepo.findByBioguideId(anyString())).thenReturn(connection.pure(Some(member(42L, "B001"))))
    val _ =
      when(m.committeeRepo.findByCode(anyString())).thenReturn(connection.pure(Some(committee(7L, "SSJU00", "Senate"))))

    val result = m.loader.load(tsv("117\tSenate\tonly-three-cols", line()), 1L).unsafeRunSync()

    val _ = result.parseErrors shouldBe 1
    val _ = result.upserted shouldBe 1
    result.rowsRead shouldBe 2
  }

  it should "create a committee from the row when the code is unknown and a name is supplied" in {
    val m = mocks()
    val _ = when(m.memberRepo.findByBioguideId(anyString())).thenReturn(connection.pure(Some(member(42L, "B001"))))
    val _ = when(m.committeeRepo.findByCode(anyString())).thenReturn(connection.pure(None))
    val _ =
      when(m.committeeRepo.upsert(any[CommitteeInsert])).thenReturn(connection.pure(committee(9L, "SSJU00", "Senate")))

    val result = m.loader.load(tsv(line()), 1L).unsafeRunSync()

    val _      = result.upserted shouldBe 1
    val captor = ArgumentCaptor.forClass(classOf[CommitteeInsert])
    val _      = verify(m.committeeRepo).upsert(captor.capture())
    captor.getValue.naturalKey shouldBe "SSJU00"
  }

  it should "fall back to a placeholder committee when the code is unknown and no name is supplied" in {
    val m = mocks()
    val _ = when(m.memberRepo.findByBioguideId(anyString())).thenReturn(connection.pure(Some(member(42L, "B001"))))
    val _ = when(m.committeeRepo.findByCode(anyString())).thenReturn(connection.pure(None))
    val _ = when(m.committeeRepo.upsertPlaceholder(anyString(), anyString()))
      .thenReturn(connection.pure(committee(9L, "XX00", "House")))

    val result = m.loader.load(tsv(line(name = "", chamber = "House", code = "XX00")), 1L).unsafeRunSync()

    val _ = result.upserted shouldBe 1
    val _ = verify(m.committeeRepo).upsertPlaceholder(eqTo("XX00"), eqTo("House"))
    verify(m.committeeRepo, never).upsert(any[CommitteeInsert])
  }

  it should "skip the header and blank lines" in {
    val m = mocks()
    val _ = when(m.memberRepo.findByBioguideId(anyString())).thenReturn(connection.pure(Some(member(42L, "B001"))))
    val _ =
      when(m.committeeRepo.findByCode(anyString())).thenReturn(connection.pure(Some(committee(7L, "SSJU00", "Senate"))))

    val withBlanks = Stream.emits(List(header, "", line(), "   "))
    val result     = m.loader.load(withBlanks, 1L).unsafeRunSync()

    result.rowsRead shouldBe 1
  }

}
