package repcheck.ingestion.votes.pipeline

import cats.effect.IO
import cats.effect.unsafe.implicits.global

import doobie._
import doobie.free.connection

import org.mockito.ArgumentMatchers.{any, anyString, eq => eqTo}
import org.mockito.Mockito.when
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar
import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}
import repcheck.ingestion.common.placeholders.{EntityRepository, PlaceholderCreator}
import repcheck.ingestion.votes.errors.MemberResolutionFailed
import repcheck.members.common.persistence.MemberRepository
import repcheck.shared.models.congress.common.{Party, UsState}
import repcheck.shared.models.congress.dos.member.MemberDO
import repcheck.shared.models.congress.dos.results.UnresolvedVotePosition
import repcheck.shared.models.congress.vote.VoteCast
import repcheck.shared.models.placeholder.HasPlaceholder

/**
 * Unit spec for [[MemberLookup]]. Covers the single-bioguide lookup, batched [[MemberLookup.resolveAll]] over a
 * position list, deduplication, and the defensive [[MemberResolutionFailed]] path.
 */
class MemberLookupSpec extends AnyFlatSpec with Matchers with MockitoSugar {

  private val testXa: Transactor[IO] = Transactor.fromDriverManager[IO](
    driver = "org.h2.Driver",
    url = "jdbc:h2:mem:memberlookup;DB_CLOSE_DELAY=-1",
    user = "",
    password = "",
    logHandler = None,
  )

  final private class StubPlaceholderCreator extends PlaceholderCreator[IO] {

    def ensureExists[T <: Product](
      naturalKey: String,
      repository: EntityRepository[IO, T],
    )(using HasPlaceholder[T]): IO[Unit] = IO.unit

  }

  private def mkLogger: PipelineLogger[IO] = {
    val m = mock[PipelineLogger[IO]]
    when(m.info(any[LogContext], anyString())).thenReturn(IO.unit)
    when(m.warn(any[LogContext], anyString())).thenReturn(IO.unit)
    when(m.error(any[LogContext], anyString(), any[Option[Throwable]])).thenReturn(IO.unit)
    m
  }

  private def mkLookup(memberRepo: MemberRepository): MemberLookup[IO] =
    new MemberLookup[IO](
      memberRepo = memberRepo,
      memberEntityRepo = mock[EntityRepository[IO, MemberDO]],
      placeholderCreator = new StubPlaceholderCreator,
      xa = testXa,
      logger = mkLogger,
    )

  private def memberDOMock(id: Long): MemberDO = {
    val m = mock[MemberDO]
    when(m.memberId).thenReturn(id)
    m
  }

  private def housePos(bioguide: String): UnresolvedVotePosition =
    UnresolvedVotePosition(
      memberSource = Left(bioguide),
      voteCast = Some(VoteCast.Yea),
      partyAtVote = Some(Party.Democrat),
      stateAtVote = Some(UsState.NewYork),
    )

  "resolveBioguide" should "return the member id after placeholder + findByBioguideId succeed" in {
    val memberRepo = mock[MemberRepository]
    val _aMember   = memberDOMock(7L)
    when(memberRepo.findByBioguideId(eqTo("A000055"))).thenReturn(connection.pure(Some(_aMember)))

    mkLookup(memberRepo).resolveBioguide("A000055", LogContext("r", "s")).unsafeRunSync() shouldBe 7L
  }

  it should "raise MemberResolutionFailed when findByBioguideId returns None after ensureExists" in {
    val memberRepo = mock[MemberRepository]
    when(memberRepo.findByBioguideId(eqTo("B000999"))).thenReturn(connection.pure(Option.empty[MemberDO]))

    val outcome = mkLookup(memberRepo).resolveBioguide("B000999", LogContext("r", "s")).attempt.unsafeRunSync()

    outcome match {
      case Left(e: MemberResolutionFailed) =>
        e.bioguideId shouldBe "B000999"
      case other => fail(s"expected Left(MemberResolutionFailed), got $other")
    }
  }

  "resolveAll" should "short-circuit on an empty position list (no DB calls)" in {
    val memberRepo = mock[MemberRepository]
    mkLookup(memberRepo).resolveAll(List.empty, LogContext("r", "s")).unsafeRunSync() shouldBe Map.empty[String, Long]
  }

  it should "return a bioguide->memberId map for a simple position list" in {
    val memberRepo = mock[MemberRepository]
    val _aMember   = memberDOMock(7L)
    val _bMember   = memberDOMock(8L)
    when(memberRepo.findByBioguideId(eqTo("A000055"))).thenReturn(connection.pure(Some(_aMember)))
    when(memberRepo.findByBioguideId(eqTo("B000999"))).thenReturn(connection.pure(Some(_bMember)))

    val map = mkLookup(memberRepo)
      .resolveAll(List(housePos("A000055"), housePos("B000999")), LogContext("r", "s"))
      .unsafeRunSync()

    map shouldBe Map("A000055" -> 7L, "B000999" -> 8L)
  }

  it should "deduplicate bioguides seen multiple times across positions" in {
    val memberRepo = mock[MemberRepository]
    val _aMember   = memberDOMock(7L)
    when(memberRepo.findByBioguideId(eqTo("A000055"))).thenReturn(connection.pure(Some(_aMember)))

    val _ = mkLookup(memberRepo)
      .resolveAll(List(housePos("A000055"), housePos("A000055"), housePos("A000055")), LogContext("r", "s"))
      .unsafeRunSync()

    import org.mockito.Mockito.{times, verify}
    verify(memberRepo, times(1)).findByBioguideId(eqTo("A000055"))
  }

  it should "ignore positions whose memberSource is Right (Senate arm)" in {
    val memberRepo = mock[MemberRepository]
    val senatePos = UnresolvedVotePosition(
      memberSource = Right("S428"),
      voteCast = Some(VoteCast.Yea),
      partyAtVote = None,
      stateAtVote = None,
    )

    val map = mkLookup(memberRepo).resolveAll(List(senatePos), LogContext("r", "s")).unsafeRunSync()

    val _ = map shouldBe Map.empty[String, Long]
    import org.mockito.Mockito.{never, verify}
    verify(memberRepo, never()).findByBioguideId(anyString())
  }

  it should "ignore positions with an empty bioguide string" in {
    val memberRepo = mock[MemberRepository]
    val emptyPos = UnresolvedVotePosition(
      memberSource = Left(""),
      voteCast = Some(VoteCast.Yea),
      partyAtVote = None,
      stateAtVote = None,
    )

    val map = mkLookup(memberRepo).resolveAll(List(emptyPos), LogContext("r", "s")).unsafeRunSync()

    val _ = map shouldBe Map.empty[String, Long]
    import org.mockito.Mockito.{never, verify}
    verify(memberRepo, never()).findByBioguideId(anyString())
  }

}
