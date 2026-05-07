package repcheck.ingestion.votes.lis

import java.time.Instant

import cats.effect.IO
import cats.effect.unsafe.implicits.global

import doobie._
import doobie.free.connection

import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.{any, anyString}
import org.mockito.Mockito.{never, times, verify, when}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar
import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}
import repcheck.members.common.persistence.LisMemberRepository
import repcheck.shared.models.congress.dos.member.LisMemberDO
import repcheck.shared.models.congress.dto.vote.{SenateVoteMemberXmlDTO, SenateVoteXmlDTO}

/**
 * Unit tests for [[DefaultLisResolver]] covering the simplified P2.3 responsibility (post-refactor): upsert one
 * `lis_members` row per distinct senator on the DTO, return `Map[naturalKey, lis_members.id]`.
 *
 * The H2 in-memory transactor is used only because `program.transact(xa)` opens a connection before executing the
 * program; every repo call is stubbed to return `connection.pure(...)`, so the underlying H2 tables are irrelevant.
 */
class LisResolverSpec extends AnyFlatSpec with Matchers with MockitoSugar {

  private val testXa: Transactor[IO] = Transactor.fromDriverManager[IO](
    driver = "org.h2.Driver",
    url = "jdbc:h2:mem:lisresolver;DB_CLOSE_DELAY=-1",
    user = "",
    password = "",
    logHandler = None,
  )

  private def createLogger(): PipelineLogger[IO] = {
    val logger = mock[PipelineLogger[IO]]
    when(logger.info(any[LogContext], anyString())).thenReturn(IO.unit)
    when(logger.warn(any[LogContext], anyString())).thenReturn(IO.unit)
    when(logger.error(any[LogContext], anyString(), any[Option[Throwable]])).thenReturn(IO.unit)
    when(logger.debug(any[LogContext], anyString())).thenReturn(IO.unit)
    logger
  }

  private case class Fixture(
    lisMemberRepo: LisMemberRepository,
    logger: PipelineLogger[IO],
  ) {

    def resolver: DefaultLisResolver[IO] =
      new DefaultLisResolver[IO](lisMemberRepo, testXa, logger)

  }

  private def makeFixture(): Fixture = Fixture(
    lisMemberRepo = mock[LisMemberRepository],
    logger = createLogger(),
  )

  private def senator(
    lisMemberId: String,
    firstName: String = "Test",
    lastName: String = "Senator",
    party: String = "D",
    state: String = "NY",
    voteCast: String = "Yea",
  ): SenateVoteMemberXmlDTO =
    SenateVoteMemberXmlDTO(
      lisMemberId = lisMemberId,
      firstName = firstName,
      lastName = lastName,
      party = party,
      state = state,
      voteCast = voteCast,
    )

  private def senateVote(members: List[SenateVoteMemberXmlDTO]): SenateVoteXmlDTO =
    SenateVoteXmlDTO(
      congress = 118,
      session = 2,
      voteNumber = 42,
      question = "On Passage",
      voteDate = "2024-06-15",
      result = "Passed",
      document = repcheck.shared.models.congress.dto.vote.SenateVoteDocumentDTO(
        documentCongress = 118,
        documentType = "S.",
        documentNumber = "42",
        documentName = "S. 42",
        documentTitle = "Fixture bill title",
        documentShortTitle = None,
        amendmentNumber = None,
        amendmentToDocumentNumber = None,
        amendmentToDocumentShortTitle = None,
      ),
      members = members,
    )

  // ============================================================================================
  // Happy path — one upsert per distinct senator, map returned natural-key → surrogate id
  // ============================================================================================

  "resolve" should "upsert one lis_members row per senator and return a natural_key -> id map" in {
    val f    = makeFixture()
    val vote = senateVote(List(senator("S1"), senator("S2"), senator("S3")))

    when(f.lisMemberRepo.upsertByNaturalKey(any[LisMemberDO])).thenAnswer { invocation =>
      val arg = invocation.getArgument[LisMemberDO](0)
      // Deterministic surrogate id based on the natural key so the assertion is stable.
      connection.pure[Long](arg.naturalKey.drop(1).toLong * 10L)
    }

    val result = f.resolver.resolve(vote).unsafeRunSync()

    val _ = result shouldBe Map("S1" -> 10L, "S2" -> 20L, "S3" -> 30L)
    verify(f.lisMemberRepo, times(3)).upsertByNaturalKey(any[LisMemberDO])
  }

  it should "pass the LIS natural key through to the upsert (no Long conversion)" in {
    val f = makeFixture()
    // Note the letter prefix — senate.gov XML identifiers are strings like 'S428' / 'S99'.
    val vote = senateVote(List(senator("S428"), senator("S99")))

    when(f.lisMemberRepo.upsertByNaturalKey(any[LisMemberDO])).thenReturn(connection.pure(1L))

    val _      = f.resolver.resolve(vote).unsafeRunSync()
    val captor = ArgumentCaptor.forClass(classOf[LisMemberDO])
    val _      = verify(f.lisMemberRepo, times(2)).upsertByNaturalKey(captor.capture())

    val keysSeen = scala.jdk.CollectionConverters.ListHasAsScala(captor.getAllValues).asScala.toList.map(_.naturalKey)
    keysSeen shouldBe List("S428", "S99")
  }

  it should "deduplicate senators with the same natural key (last-wins)" in {
    val f = makeFixture()
    val vote = senateVote(
      List(
        senator("S5", firstName = "First"),
        senator("S5", firstName = "Second"),
        senator("S6"),
      )
    )

    when(f.lisMemberRepo.upsertByNaturalKey(any[LisMemberDO])).thenAnswer { invocation =>
      val arg = invocation.getArgument[LisMemberDO](0)
      connection.pure[Long](arg.naturalKey.drop(1).toLong)
    }

    val result = f.resolver.resolve(vote).unsafeRunSync()

    // Only 2 upserts — S5 was collapsed — and the "Second" entry won the dedup.
    val _      = verify(f.lisMemberRepo, times(2)).upsertByNaturalKey(any[LisMemberDO])
    val _      = result shouldBe Map("S5" -> 5L, "S6" -> 6L)
    val captor = ArgumentCaptor.forClass(classOf[LisMemberDO])
    val _      = verify(f.lisMemberRepo, times(2)).upsertByNaturalKey(captor.capture())
    val s5Row =
      scala.jdk.CollectionConverters.ListHasAsScala(captor.getAllValues).asScala.toList.find(_.naturalKey == "S5")
    s5Row match {
      case Some(row) => row.firstName shouldBe Some("Second")
      case None      => fail("expected an upsert for S5")
    }
  }

  // ============================================================================================
  // DTO -> DO field mapping: empty strings become None, non-empty stay Some
  // ============================================================================================

  it should "stamp lastVerified within the last few seconds on every upsert" in {
    val f    = makeFixture()
    val vote = senateVote(List(senator("S7")))

    when(f.lisMemberRepo.upsertByNaturalKey(any[LisMemberDO])).thenReturn(connection.pure(700L))

    val before = Instant.now()
    val _      = f.resolver.resolve(vote).unsafeRunSync()
    val after  = Instant.now().plusSeconds(2)

    val captor = ArgumentCaptor.forClass(classOf[LisMemberDO])
    val _      = verify(f.lisMemberRepo).upsertByNaturalKey(captor.capture())
    val stamp  = captor.getValue.lastVerified
    val _      = stamp.isDefined shouldBe true
    stamp.foreach { ts =>
      val _ = ts.isBefore(before) shouldBe false
      ts.isAfter(after) shouldBe false
    }
  }

  it should "treat empty-string DTO fields as None on the LisMemberDO" in {
    val f    = makeFixture()
    val s    = senator("S10", firstName = "", lastName = "", party = "", state = "")
    val vote = senateVote(List(s))

    when(f.lisMemberRepo.upsertByNaturalKey(any[LisMemberDO])).thenReturn(connection.pure(100L))

    val _      = f.resolver.resolve(vote).unsafeRunSync()
    val captor = ArgumentCaptor.forClass(classOf[LisMemberDO])
    val _      = verify(f.lisMemberRepo).upsertByNaturalKey(captor.capture())
    val row    = captor.getValue

    val _ = row.firstName shouldBe None
    val _ = row.lastName shouldBe None
    val _ = row.party shouldBe None
    val _ = row.state shouldBe None
    row.naturalKey shouldBe "S10"
  }

  it should "pass firstName/lastName/party/state through when populated" in {
    val f    = makeFixture()
    val s    = senator("S11", firstName = "Jane", lastName = "Roe", party = "R", state = "TX")
    val vote = senateVote(List(s))

    when(f.lisMemberRepo.upsertByNaturalKey(any[LisMemberDO])).thenReturn(connection.pure(110L))

    val _      = f.resolver.resolve(vote).unsafeRunSync()
    val captor = ArgumentCaptor.forClass(classOf[LisMemberDO])
    val _      = verify(f.lisMemberRepo).upsertByNaturalKey(captor.capture())
    val row    = captor.getValue

    val _ = row.firstName shouldBe Some("Jane")
    val _ = row.lastName shouldBe Some("Roe")
    val _ = row.party shouldBe Some("R")
    row.state shouldBe Some("TX")
  }

  // ============================================================================================
  // Empty DTO short-circuit
  // ============================================================================================

  it should "handle an empty senators list without issuing any upsert" in {
    val f    = makeFixture()
    val vote = senateVote(List.empty)

    val result = f.resolver.resolve(vote).unsafeRunSync()

    val _ = result shouldBe empty
    verify(f.lisMemberRepo, never()).upsertByNaturalKey(any[LisMemberDO])
  }

  // ============================================================================================
  // Logging
  // ============================================================================================

  it should "log upsert count at info level when at least one senator was present" in {
    val f    = makeFixture()
    val vote = senateVote(List(senator("S50"), senator("S51")))

    when(f.lisMemberRepo.upsertByNaturalKey(any[LisMemberDO])).thenReturn(connection.pure(1L))

    val _ = f.resolver.resolve(vote).unsafeRunSync()

    val ctxCaptor = ArgumentCaptor.forClass(classOf[LogContext])
    val msgCaptor = ArgumentCaptor.forClass(classOf[String])
    val _         = verify(f.logger, times(1)).info(ctxCaptor.capture(), msgCaptor.capture())

    val msg = msgCaptor.getValue
    // Only 1 row is returned because both upserts stub the same id; the log still reflects the result map size.
    val _ = msg should include("Upserted")
    val _ = msg should include("118-2-42") // congress-session-voteNumber label
    ctxCaptor.getValue.stepName shouldBe "votes-lis-resolver"
  }

  it should "emit no log when the senators list is empty" in {
    val f    = makeFixture()
    val vote = senateVote(List.empty)

    val _ = f.resolver.resolve(vote).unsafeRunSync()

    verify(f.logger, never()).info(any[LogContext], anyString())
  }

  // ============================================================================================
  // Error propagation — no silent swallowing
  // ============================================================================================

  it should "propagate a repository failure as a failed effect" in {
    val f     = makeFixture()
    val vote  = senateVote(List(senator("S99")))
    val error = new RuntimeException("DB unreachable")

    when(f.lisMemberRepo.upsertByNaturalKey(any[LisMemberDO])).thenReturn(connection.raiseError[Long](error))

    val thrown = intercept[RuntimeException] {
      val _ = f.resolver.resolve(vote).unsafeRunSync()
    }
    thrown.getMessage shouldBe "DB unreachable"
  }

  // ============================================================================================
  // Companion apply factory produces a working resolver
  // ============================================================================================

  "LisResolver.apply" should "construct a DefaultLisResolver wired to the supplied dependencies" in {
    val f    = makeFixture()
    val r    = LisResolver[IO](f.lisMemberRepo, testXa, f.logger)
    val vote = senateVote(List.empty)

    val result = r.resolve(vote).unsafeRunSync()
    result shouldBe empty
  }

  // ============================================================================================
  // Pure helpers
  // ============================================================================================

  "buildRunLabel" should "format congress/session/voteNumber as dash-separated string" in {
    val f    = makeFixture()
    val vote = senateVote(List.empty).copy(congress = 117, session = 1, voteNumber = 99)
    f.resolver.buildRunLabel(vote) shouldBe "117-1-99"
  }

  "dedupSenators" should "preserve order of first-seen natural keys while last-wins on conflicts" in {
    val f = makeFixture()
    val vote = senateVote(
      List(
        senator("A", firstName = "a1"),
        senator("B", firstName = "b1"),
        senator("A", firstName = "a2"),
        senator("C", firstName = "c1"),
      )
    )

    val rows = f.resolver.dedupSenators(vote)

    // Insertion order preserved for A-B-C; A's last record wins on firstName.
    val _ = rows.map(_.naturalKey) shouldBe List("A", "B", "C")
    val _ = rows.headOption.map(_.firstName) shouldBe Some("a2")
    ()
  }

  "buildLisMemberDO" should "copy senator row fields and stamp lastVerified" in {
    val f   = makeFixture()
    val now = Instant.parse("2026-04-15T12:00:00Z")
    val row = SenatorRow(naturalKey = "S200", firstName = "Alex", lastName = "Kim", party = "D", state = "CA")

    val dto = f.resolver.buildLisMemberDO(row, now)

    val _ = dto.id shouldBe 0L
    val _ = dto.naturalKey shouldBe "S200"
    val _ = dto.firstName shouldBe Some("Alex")
    val _ = dto.lastName shouldBe Some("Kim")
    val _ = dto.party shouldBe Some("D")
    val _ = dto.state shouldBe Some("CA")
    val _ = dto.lastVerified shouldBe Some(now)
    dto.createdAt shouldBe None
  }

  "logUpserts" should "emit nothing when the result map is empty" in {
    val f    = makeFixture()
    val vote = senateVote(List.empty)
    val ctx  = LogContext(runId = "ctx-1", stepName = "votes-lis-resolver")

    val _ = f.resolver.logUpserts(ctx, vote, Map.empty[String, Long]).unsafeRunSync()

    verify(f.logger, never()).info(any[LogContext], anyString())
  }

  it should "render the upsert count and vote label when non-empty" in {
    val f    = makeFixture()
    val vote = senateVote(List.empty).copy(congress = 118, session = 2, voteNumber = 50)
    val ctx  = LogContext(runId = vote.voteNumber.toString, stepName = "votes-lis-resolver")

    val _ = f.resolver.logUpserts(ctx, vote, Map("S1" -> 10L, "S2" -> 20L, "S3" -> 30L)).unsafeRunSync()

    val msgCaptor = ArgumentCaptor.forClass(classOf[String])
    val _         = verify(f.logger, times(1)).info(any[LogContext], msgCaptor.capture())
    val msg       = msgCaptor.getValue
    val _         = msg should include("Upserted 3 lis_members rows")
    msg should include("118-2-50")
  }

}
