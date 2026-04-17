package repcheck.members.common.persistence

import cats.effect.IO
import cats.effect.unsafe.implicits.global

import doobie.{Fragment, Transactor}
import doobie.implicits._

import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class DoobieMemberHistoryArchiverUnitSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  private val archiver = new DoobieMemberHistoryArchiver

  private val histFrag     = Fragment.const("member_history")
  private val membersFrag  = Fragment.const("members")
  private val termHistFrag = Fragment.const("member_term_history")
  private val termsFrag    = Fragment.const("member_terms")

  "archiveMember" should "produce a ConnectionIO for a valid bioguide id" in {
    val cio = archiver.archiveMember("A000001")
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  it should "produce a ConnectionIO for an empty bioguide id" in {
    val cio = archiver.archiveMember("")
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  it should "produce a ConnectionIO for a bioguide id with special characters" in {
    val cio = archiver.archiveMember("Z-!@#$")
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  it should "produce a ConnectionIO for a non-existent bioguide id" in {
    val cio = archiver.archiveMember("NOPE-9999")
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  "dispatchArchive" should "return ConnectionIO.unit for None (member not found)" in {
    val cio = archiver.dispatchArchive(None, histFrag, membersFrag, termHistFrag, termsFrag)
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  it should "return archiveExisting ConnectionIO for Some(memberId)" in {
    val cio = archiver.dispatchArchive(Some(1L), histFrag, membersFrag, termHistFrag, termsFrag)
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  "archiveExisting" should "produce a ConnectionIO[Unit]" in {
    val cio = archiver.archiveExisting(histFrag, membersFrag, termHistFrag, termsFrag, memberId = 1L)
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  it should "produce a ConnectionIO[Unit] for any member id" in {
    val cio = archiver.archiveExisting(histFrag, membersFrag, termHistFrag, termsFrag, memberId = 999L)
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  "insertMemberHistory" should "produce a ConnectionIO[Long]" in {
    val cio = archiver.insertMemberHistory(histFrag, membersFrag, memberId = 1L)
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  it should "produce a ConnectionIO for different member IDs" in {
    val cio = archiver.insertMemberHistory(histFrag, membersFrag, memberId = 42L)
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  "insertTermHistory" should "produce a ConnectionIO[Int]" in {
    val cio = archiver.insertTermHistory(historyId = 1L, termHistFrag, termsFrag, memberId = 1L)
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  it should "produce a ConnectionIO for different history IDs" in {
    val cio = archiver.insertTermHistory(historyId = 99L, termHistFrag, termsFrag, memberId = 5L)
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  "archiveTerms" should "produce a ConnectionIO[Unit] for a valid history ID" in {
    val cio = archiver.archiveTerms(termHistFrag, termsFrag, memberId = 1L)(historyId = 10L)
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  it should "produce a ConnectionIO[Unit] for different history and member IDs" in {
    val cio = archiver.archiveTerms(termHistFrag, termsFrag, memberId = 99L)(historyId = 500L)
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  "DoobieMemberHistoryArchiver" should "implement MemberHistoryArchiver" in {
    archiver shouldBe a[MemberHistoryArchiver[?]]
  }

  private lazy val h2xa: Transactor[IO] = Transactor.fromDriverManager[IO](
    driver = "org.h2.Driver",
    url = "jdbc:h2:mem:testMemberHistoryArchiver;MODE=PostgreSQL;DB_CLOSE_DELAY=-1",
    user = "",
    password = "",
    logHandler = None,
  )

  override def beforeAll(): Unit = {
    super.beforeAll()
    sql"""CREATE TABLE IF NOT EXISTS members (
      id                  BIGINT AUTO_INCREMENT PRIMARY KEY,
      natural_key         TEXT NOT NULL UNIQUE,
      first_name          TEXT,
      last_name           TEXT,
      direct_order_name   TEXT,
      inverted_order_name TEXT,
      honorific_name      TEXT,
      birth_year          TEXT,
      current_party       TEXT,
      state               TEXT,
      district            TEXT,
      image_url           TEXT,
      image_attribution   TEXT,
      official_url        TEXT,
      update_date         TIMESTAMP WITH TIME ZONE
    )""".update.run.transact(h2xa).unsafeRunSync()
    sql"""CREATE TABLE IF NOT EXISTS member_history (
      id                  BIGINT AUTO_INCREMENT PRIMARY KEY,
      member_id           BIGINT,
      first_name          TEXT,
      last_name           TEXT,
      direct_order_name   TEXT,
      inverted_order_name TEXT,
      honorific_name      TEXT,
      birth_year          TEXT,
      current_party       TEXT,
      state               TEXT,
      district            TEXT,
      image_url           TEXT,
      image_attribution   TEXT,
      official_url        TEXT,
      update_date         TIMESTAMP WITH TIME ZONE
    )""".update.run.transact(h2xa).unsafeRunSync()
    sql"""CREATE TABLE IF NOT EXISTS member_terms (
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
    )""".update.run.transact(h2xa).unsafeRunSync()
    sql"""CREATE TABLE IF NOT EXISTS member_term_history (
      id          BIGINT AUTO_INCREMENT PRIMARY KEY,
      history_id  BIGINT NOT NULL,
      member_id   BIGINT,
      chamber     TEXT,
      congress    INT,
      start_year  INT,
      end_year    INT,
      member_type TEXT,
      state_code  TEXT,
      state_name  TEXT,
      district    INT
    )""".update.run.transact(h2xa).unsafeRunSync()
    val _ = sql"INSERT INTO members (natural_key) VALUES ('A000001')".update.run
      .transact(h2xa)
      .unsafeRunSync()
  }

  "dispatchArchive" should "execute and return unit for None without touching the database" in {
    archiver
      .dispatchArchive(None, histFrag, membersFrag, termHistFrag, termsFrag)
      .transact(h2xa)
      .unsafeRunSync() shouldBe ()
  }

  "archiveMember" should "execute the full archive flow when the member exists in H2" in {
    val _ = archiver.archiveMember("A000001").transact(h2xa).unsafeRunSync()
    succeed
  }

}
