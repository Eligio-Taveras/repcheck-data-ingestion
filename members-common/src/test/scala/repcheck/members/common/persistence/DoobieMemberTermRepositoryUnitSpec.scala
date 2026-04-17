package repcheck.members.common.persistence

import cats.effect.IO
import cats.effect.unsafe.implicits.global

import doobie.Transactor
import doobie.implicits._

import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import repcheck.shared.models.congress.common.{Chamber, UsState}
import repcheck.shared.models.congress.dos.member.MemberTermDO
import repcheck.shared.models.congress.member.MemberType

class DoobieMemberTermRepositoryUnitSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  private val repo = new DoobieMemberTermRepository

  private val sampleTerm = MemberTermDO(
    termId = 0L,
    memberId = 1L,
    chamber = Some(Chamber.House),
    congress = Some(118),
    startYear = Some(2023),
    endYear = Some(2025),
    memberType = Some(MemberType.Representative),
    stateCode = Some(UsState.NewYork),
    stateName = Some("New York"),
    district = Some(5),
  )

  "replaceAll" should "produce a ConnectionIO describing the delete-then-insert" in {
    val cio = repo.replaceAll(1L, List(sampleTerm))
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  it should "handle an empty term list" in {
    val cio = repo.replaceAll(1L, List.empty)
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  it should "handle multiple terms" in {
    val second = sampleTerm.copy(congress = Some(117), startYear = Some(2021), endYear = Some(2023))
    val cio    = repo.replaceAll(1L, List(sampleTerm, second))
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  it should "handle terms with all optional fields set to None" in {
    val minimal = MemberTermDO(
      termId = 0L,
      memberId = 1L,
      chamber = None,
      congress = None,
      startYear = None,
      endYear = None,
      memberType = None,
      stateCode = None,
      stateName = None,
      district = None,
    )
    val cio = repo.replaceAll(1L, List(minimal))
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  it should "accept Senate terms" in {
    val senateTerm = sampleTerm.copy(
      chamber = Some(Chamber.Senate),
      memberType = Some(MemberType.Senator),
      district = None,
    )
    val cio = repo.replaceAll(2L, List(senateTerm))
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  "findByMemberId" should "produce a ConnectionIO for a valid member ID" in {
    val cio = repo.findByMemberId(1L)
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  it should "produce a ConnectionIO for a zero ID" in {
    val cio = repo.findByMemberId(0L)
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  "buildRows" should "map a single House term into an insert row" in {
    val rows    = repo.buildRows(1L, List(sampleTerm))
    val _       = rows.length shouldBe 1
    val firstId = rows.headOption.map(_._1)
    firstId shouldBe Some(1L)
  }

  it should "return an empty list for an empty term list" in {
    repo.buildRows(1L, List.empty) shouldBe empty
  }

  it should "map multiple terms preserving order" in {
    val second = sampleTerm.copy(congress = Some(117), startYear = Some(2021))
    val rows   = repo.buildRows(1L, List(sampleTerm, second))
    val _      = rows.length shouldBe 2
    val _      = rows(0)._3 shouldBe Some(118)
    rows(1)._3 shouldBe Some(117)
  }

  it should "use the provided memberId for all rows" in {
    val second = sampleTerm.copy(congress = Some(117))
    val rows   = repo.buildRows(99L, List(sampleTerm, second))
    rows.forall(_._1 == 99L) shouldBe true
  }

  it should "map optional fields to None when missing" in {
    val minimal  = MemberTermDO(0L, 1L, None, None, None, None, None, None, None, None)
    val rows     = repo.buildRows(1L, List(minimal))
    val _        = rows.length shouldBe 1
    val chamber  = rows.headOption.map(_._2)
    val congress = rows.headOption.map(_._3)
    val _        = chamber shouldBe Some(None)
    congress shouldBe Some(None)
  }

  it should "include district for House terms" in {
    val rows     = repo.buildRows(1L, List(sampleTerm))
    val district = rows.headOption.map(_._9)
    district shouldBe Some(Some(5))
  }

  it should "pass None district for Senate terms" in {
    val senate = sampleTerm.copy(chamber = Some(Chamber.Senate), memberType = Some(MemberType.Senator), district = None)
    val rows   = repo.buildRows(1L, List(senate))
    val district = rows.headOption.map(_._9)
    district shouldBe Some(None)
  }

  "runInsert" should "produce a ConnectionIO[Int] for a non-empty row list" in {
    val rows = repo.buildRows(1L, List(sampleTerm))
    val cio  = repo.runInsert(rows)
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  it should "produce a ConnectionIO[Int] for an empty row list" in {
    val cio = repo.runInsert(List.empty)
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  "DoobieMemberTermRepository" should "implement MemberTermRepository trait" in {
    repo shouldBe a[MemberTermRepository]
  }

  private lazy val h2xa: Transactor[IO] = Transactor.fromDriverManager[IO](
    driver = "org.h2.Driver",
    url = "jdbc:h2:mem:testMemberTerm;MODE=PostgreSQL;DB_CLOSE_DELAY=-1",
    user = "",
    password = "",
    logHandler = None,
  )

  override def beforeAll(): Unit = {
    super.beforeAll()
    val _ = sql"""
      CREATE TABLE IF NOT EXISTS member_terms (
        id          BIGINT AUTO_INCREMENT PRIMARY KEY,
        member_id   BIGINT NOT NULL,
        chamber     TEXT,
        congress    INT,
        start_year  INT,
        end_year    INT,
        member_type TEXT,
        state_code  TEXT,
        state_name  TEXT,
        district    INT
      )
    """.update.run.transact(h2xa).unsafeRunSync()
  }

  "replaceAll" should "execute delete and insert steps when run with a transactor" in {
    repo.replaceAll(99L, List.empty).transact(h2xa).unsafeRunSync() shouldBe ()
  }

}
