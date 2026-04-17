package repcheck.ingestion.bills.common.persistence

import cats.effect.IO
import cats.effect.unsafe.implicits.global

import doobie.Transactor
import doobie.implicits._

import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class DoobieBillHistoryArchiverUnitSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  private val archiver = new DoobieBillHistoryArchiver

  "archiveBill" should "produce a ConnectionIO[Long] for a standard HR bill" in {
    val cio = archiver.archiveBill("118-HR-300")
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  it should "produce a ConnectionIO[Long] for a Senate bill" in {
    val cio = archiver.archiveBill("117-S-42")
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  it should "produce a ConnectionIO[Long] for a joint resolution" in {
    val cio = archiver.archiveBill("118-SJRES-5")
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  "parseNaturalKey" should "split a standard HR bill into congress, type, and number" in {
    val (congress, billType, number) = archiver.parseNaturalKey("118-HR-300")
    val _                            = congress shouldBe 118
    val _                            = billType shouldBe "hr"
    number shouldBe "300"
  }

  it should "lowercase the bill type" in {
    val (_, billType, _) = archiver.parseNaturalKey("118-HR-300")
    billType shouldBe "hr"
  }

  it should "parse Senate bills" in {
    val (congress, billType, number) = archiver.parseNaturalKey("117-S-42")
    val _                            = congress shouldBe 117
    val _                            = billType shouldBe "s"
    number shouldBe "42"
  }

  it should "parse joint resolutions" in {
    val (congress, billType, number) = archiver.parseNaturalKey("118-SJRES-5")
    val _                            = congress shouldBe 118
    val _                            = billType shouldBe "sjres"
    number shouldBe "5"
  }

  it should "treat everything after the second dash as the bill number" in {
    val (_, _, number) = archiver.parseNaturalKey("118-HR-1234")
    number shouldBe "1234"
  }

  "buildFragments" should "return an ArchiveFragments instance" in {
    val frags = archiver.buildFragments()
    frags shouldBe a[DoobieBillHistoryArchiver#ArchiveFragments]
  }

  it should "expose a billsTable fragment referencing 'bills'" in {
    archiver.buildFragments().billsTable.toString should include("bills")
  }

  it should "expose a histTable fragment referencing 'bill_history'" in {
    archiver.buildFragments().histTable.toString should include("bill_history")
  }

  it should "expose a cospHistTable fragment referencing 'bill_cosponsor_history'" in {
    archiver.buildFragments().cospHistTable.toString should include("bill_cosponsor_history")
  }

  it should "expose a subjHistTable fragment referencing 'bill_subject_history'" in {
    archiver.buildFragments().subjHistTable.toString should include("bill_subject_history")
  }

  "insertBillHistory" should "produce a ConnectionIO for a standard bill" in {
    val frags = archiver.buildFragments()
    val cio   = archiver.insertBillHistory(118, "hr", "300", frags)
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  it should "produce a ConnectionIO for a different congress and type" in {
    val frags = archiver.buildFragments()
    val cio   = archiver.insertBillHistory(117, "s", "42", frags)
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  "insertCosponsorHistory" should "produce a ConnectionIO for a standard bill" in {
    val frags = archiver.buildFragments()
    val cio   = archiver.insertCosponsorHistory(historyId = 1L, 118, "hr", "300", frags)
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  it should "produce a ConnectionIO for a different history ID" in {
    val frags = archiver.buildFragments()
    val cio   = archiver.insertCosponsorHistory(historyId = 999L, 117, "s", "42", frags)
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  "insertSubjectHistory" should "produce a ConnectionIO for a standard bill" in {
    val frags = archiver.buildFragments()
    val cio   = archiver.insertSubjectHistory(historyId = 1L, 118, "hr", "300", frags)
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  it should "produce a ConnectionIO for a different history ID" in {
    val frags = archiver.buildFragments()
    val cio   = archiver.insertSubjectHistory(historyId = 42L, 117, "s", "10", frags)
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  "archiveDependents" should "produce a ConnectionIO[Long] for a standard bill" in {
    val frags = archiver.buildFragments()
    val cio   = archiver.archiveDependents(118, "hr", "300", frags)(1L)
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  it should "produce a ConnectionIO[Long] for a Senate bill" in {
    val frags = archiver.buildFragments()
    val cio   = archiver.archiveDependents(117, "s", "42", frags)(99L)
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  it should "produce a ConnectionIO[Long] for a joint resolution" in {
    val frags = archiver.buildFragments()
    val cio   = archiver.archiveDependents(118, "sjres", "5", frags)(1000L)
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  "DoobieBillHistoryArchiver" should "implement the BillHistoryArchiver trait" in {
    archiver shouldBe a[BillHistoryArchiver[?]]
  }

  private lazy val h2xa: Transactor[IO] = Transactor.fromDriverManager[IO](
    driver = "org.h2.Driver",
    url = "jdbc:h2:mem:testBillHistoryArchiver;MODE=PostgreSQL;DB_CLOSE_DELAY=-1",
    user = "",
    password = "",
    logHandler = None,
  )

  override def beforeAll(): Unit = {
    super.beforeAll()
    val _ = sql"""CREATE TABLE IF NOT EXISTS bills (
      id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
      congress INT NOT NULL, bill_type TEXT NOT NULL, number INT NOT NULL,
      title TEXT, origin_chamber TEXT, origin_chamber_code TEXT,
      introduced_date DATE, policy_area TEXT, latest_action_date DATE,
      latest_action_text TEXT, constitutional_authority_text TEXT,
      sponsor_member_id BIGINT, text_url TEXT, text_format TEXT,
      text_version_type TEXT, text_date DATE,
      summary_text TEXT, summary_action_desc TEXT, summary_action_date DATE,
      update_date TIMESTAMP WITH TIME ZONE,
      update_date_including_text TIMESTAMP WITH TIME ZONE,
      legislation_url TEXT, api_url TEXT
    )""".update.run.transact(h2xa).unsafeRunSync()
    val _ = sql"""CREATE TABLE IF NOT EXISTS bill_history (
      id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
      bill_id BIGINT, congress INT, bill_type TEXT, number INT,
      title TEXT, origin_chamber TEXT, origin_chamber_code TEXT,
      introduced_date DATE, policy_area TEXT, latest_action_date DATE,
      latest_action_text TEXT, constitutional_authority_text TEXT,
      sponsor_member_id BIGINT, text_url TEXT, text_format TEXT,
      text_version_type TEXT, text_date DATE,
      summary_text TEXT, summary_action_desc TEXT, summary_action_date DATE,
      update_date TIMESTAMP WITH TIME ZONE,
      update_date_including_text TIMESTAMP WITH TIME ZONE,
      legislation_url TEXT, api_url TEXT
    )""".update.run.transact(h2xa).unsafeRunSync()
    val _ = sql"""CREATE TABLE IF NOT EXISTS bill_cosponsors (
      id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
      bill_id BIGINT NOT NULL, member_id BIGINT NOT NULL,
      is_original_cosponsor BOOLEAN, sponsorship_date DATE
    )""".update.run.transact(h2xa).unsafeRunSync()
    val _ = sql"""CREATE TABLE IF NOT EXISTS bill_cosponsor_history (
      id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
      history_id BIGINT NOT NULL, bill_id BIGINT,
      member_id BIGINT, is_original_cosponsor BOOLEAN, sponsorship_date DATE
    )""".update.run.transact(h2xa).unsafeRunSync()
    val _ = sql"""CREATE TABLE IF NOT EXISTS bill_subjects (
      id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
      bill_id BIGINT NOT NULL, subject_name TEXT NOT NULL,
      embedding TEXT, update_date TIMESTAMP WITH TIME ZONE
    )""".update.run.transact(h2xa).unsafeRunSync()
    val _ = sql"""CREATE TABLE IF NOT EXISTS bill_subject_history (
      id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
      history_id BIGINT NOT NULL, bill_id BIGINT,
      subject_name TEXT, update_date TIMESTAMP WITH TIME ZONE
    )""".update.run.transact(h2xa).unsafeRunSync()
    val _ =
      sql"INSERT INTO bills (congress, bill_type, number) VALUES (118, 'hr', 300)".update.run
        .transact(h2xa)
        .unsafeRunSync()
  }

}
