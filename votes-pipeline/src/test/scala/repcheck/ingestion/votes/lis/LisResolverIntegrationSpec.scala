package repcheck.ingestion.votes.lis

import cats.effect.IO
import cats.effect.unsafe.implicits.global

import doobie.implicits._

import org.mockito.ArgumentMatchers.{any, anyString}
import org.mockito.Mockito.when
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar
import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}
import repcheck.members.common.persistence.DoobieLisMemberRepository
import repcheck.members.common.testing.{DockerRequired, TransactorFixture}
import repcheck.shared.models.congress.dto.vote.{SenateVoteMemberXmlDTO, SenateVoteXmlDTO}

/**
 * Integration tests for [[DefaultLisResolver]] running against a real AlloyDB Omni container via [[TransactorFixture]].
 * Confirms the post-refactor contract: votes-pipeline upserts `lis_members` rows only, without touching `members` or
 * `member_lis_mapping` in any scenario.
 */
class LisResolverIntegrationSpec extends AnyFlatSpec with Matchers with TransactorFixture with MockitoSugar {

  private val realLisMemberRepo = new DoobieLisMemberRepository

  private def createLogger(): PipelineLogger[IO] = {
    val logger = mock[PipelineLogger[IO]]
    when(logger.info(any[LogContext], anyString())).thenReturn(IO.unit)
    when(logger.warn(any[LogContext], anyString())).thenReturn(IO.unit)
    when(logger.error(any[LogContext], anyString(), any[Option[Throwable]])).thenReturn(IO.unit)
    when(logger.debug(any[LogContext], anyString())).thenReturn(IO.unit)
    logger
  }

  private def senator(
    lisMemberId: String,
    firstName: String,
    lastName: String = "Senator",
    party: String = "D",
    state: String = "NY",
  ): SenateVoteMemberXmlDTO =
    SenateVoteMemberXmlDTO(
      lisMemberId = lisMemberId,
      firstName = firstName,
      lastName = lastName,
      party = party,
      state = state,
      voteCast = "Yea",
    )

  private def senateVote(members: List[SenateVoteMemberXmlDTO]): SenateVoteXmlDTO =
    SenateVoteXmlDTO(
      congress = 118,
      session = 2,
      voteNumber = 100,
      question = "On Passage",
      voteDate = "2024-06-15",
      result = "Passed",
      document = repcheck.shared.models.congress.dto.vote.SenateVoteDocumentDTO(
        documentCongress = 118,
        documentType = "S.",
        documentNumber = "100",
        documentName = "S. 100",
        documentTitle = "Integration fixture bill title",
        documentShortTitle = None,
      ),
      members = members,
    )

  private def membersRowCount: Int =
    sql"SELECT COUNT(*) FROM members".query[Int].unique.transact(xa).unsafeRunSync()

  private def mappingRowCount: Int =
    sql"SELECT COUNT(*) FROM member_lis_mapping".query[Int].unique.transact(xa).unsafeRunSync()

  private def lisMemberRowCount: Int =
    sql"SELECT COUNT(*) FROM lis_members".query[Int].unique.transact(xa).unsafeRunSync()

  "resolve" should "insert a lis_members row per senator and touch nothing else" taggedAs DockerRequired in {
    val vote = senateVote(
      List(
        senator("S1001", firstName = "Jane", lastName = "Doe", party = "D", state = "NY"),
        senator("S1002", firstName = "John", lastName = "Smith", party = "R", state = "TX"),
        senator("S1003", firstName = "Alex", lastName = "Kim", party = "I", state = "VT"),
      )
    )
    val resolver = new DefaultLisResolver[IO](realLisMemberRepo, xa, createLogger())

    val result = resolver.resolve(vote).unsafeRunSync()

    val _ = result.keySet shouldBe Set("S1001", "S1002", "S1003")
    val _ = result.values.forall(_ > 0L) shouldBe true
    val _ = lisMemberRowCount shouldBe 3
    val _ = membersRowCount shouldBe 0
    mappingRowCount shouldBe 0
  }

  it should "return the same surrogate id on re-resolution of the same vote (idempotent)" taggedAs DockerRequired in {
    val vote     = senateVote(List(senator("S2001", firstName = "First")))
    val resolver = new DefaultLisResolver[IO](realLisMemberRepo, xa, createLogger())

    val first  = resolver.resolve(vote).unsafeRunSync()
    val second = resolver.resolve(vote).unsafeRunSync()

    val _ = first shouldBe second
    val _ = lisMemberRowCount shouldBe 1
    val _ = membersRowCount shouldBe 0
    mappingRowCount shouldBe 0
  }

  it should "update mutable fields on re-resolution (later senate.gov snapshot overwrites earlier)" taggedAs DockerRequired in {
    val firstVote  = senateVote(List(senator("S3001", firstName = "Initial", state = "NY")))
    val secondVote = senateVote(List(senator("S3001", firstName = "Updated", state = "CA")))
    val resolver   = new DefaultLisResolver[IO](realLisMemberRepo, xa, createLogger())

    val _     = resolver.resolve(firstVote).unsafeRunSync()
    val _     = resolver.resolve(secondVote).unsafeRunSync()
    val found = realLisMemberRepo.findByNaturalKey("S3001").transact(xa).unsafeRunSync()

    found match {
      case Some(row) =>
        val _ = row.firstName shouldBe Some("Updated")
        row.state shouldBe Some("CA")
      case None => fail("expected lis_members row for S3001")
    }
  }

  it should "dedup senators with the same natural_key to a single upsert" taggedAs DockerRequired in {
    val vote = senateVote(
      List(
        senator("S4001", firstName = "One"),
        senator("S4001", firstName = "Two"),
        senator("S4001", firstName = "Three"),
      )
    )
    val resolver = new DefaultLisResolver[IO](realLisMemberRepo, xa, createLogger())

    val result = resolver.resolve(vote).unsafeRunSync()

    val _ = result.keySet shouldBe Set("S4001")
    val _ = lisMemberRowCount shouldBe 1
    // Last-wins dedup means "Three" is the persisted firstName.
    val found = realLisMemberRepo.findByNaturalKey("S4001").transact(xa).unsafeRunSync()
    found.flatMap(_.firstName) shouldBe Some("Three")
  }

  it should "short-circuit when the DTO has no senators" taggedAs DockerRequired in {
    val resolver = new DefaultLisResolver[IO](realLisMemberRepo, xa, createLogger())

    val result = resolver.resolve(senateVote(List.empty)).unsafeRunSync()

    val _ = result shouldBe empty
    lisMemberRowCount shouldBe 0
  }

}
