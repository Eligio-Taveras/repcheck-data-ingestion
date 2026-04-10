package com.repcheck.bills.common.testing

import cats.effect.IO

import doobie.Transactor
import doobie.implicits._

import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Suite}

/**
 * Provides a shared AlloyDB Omni container and Doobie transactor for Docker-backed integration tests. All suites share
 * one container (via [[SharedDockerPostgres]]) and run sequentially (`Test / parallelExecution := false` in build.sbt)
 * to avoid cross-suite FK violations during cleanup.
 */
trait TransactorFixture extends BeforeAndAfterAll with BeforeAndAfterEach { self: Suite =>

  import cats.effect.unsafe.implicits.global

  protected lazy val containerInfo: PostgresContainerInfo = SharedDockerPostgres.info

  protected lazy val xa: Transactor[IO] = Transactor.fromDriverManager[IO](
    driver = "org.postgresql.Driver",
    url = containerInfo.jdbcUrl,
    user = containerInfo.user,
    password = containerInfo.password,
    logHandler = None,
  )

  override def beforeAll(): Unit = {
    super.beforeAll()
    val _ = containerInfo
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    seedMembers()
  }

  override def afterEach(): Unit = {
    cleanTables()
    super.afterEach()
  }

  /**
   * Insert placeholder member rows so FK constraints on `bill_cosponsors.member_id` are satisfied. Uses ON CONFLICT to
   * be idempotent across tests.
   */
  private def seedMembers(): Unit = {
    val _ = sql"""
      INSERT INTO members (natural_key) VALUES ('TEST-M001')
        ON CONFLICT (natural_key) DO NOTHING;
      INSERT INTO members (natural_key) VALUES ('TEST-M002')
        ON CONFLICT (natural_key) DO NOTHING;
      INSERT INTO members (natural_key) VALUES ('TEST-M003')
        ON CONFLICT (natural_key) DO NOTHING;
      INSERT INTO members (natural_key) VALUES ('TEST-M004')
        ON CONFLICT (natural_key) DO NOTHING;
      INSERT INTO members (natural_key) VALUES ('TEST-M005')
        ON CONFLICT (natural_key) DO NOTHING;
    """.update.run.transact(xa).unsafeRunSync()
  }

  /** Look up the auto-generated `members.id` for a given natural key. */
  protected def memberIdByKey(key: String): Long =
    sql"SELECT id FROM members WHERE natural_key = $key"
      .query[Long]
      .unique
      .transact(xa)
      .unsafeRunSync()

  private def cleanTables(): Unit = {
    val _ = sql"""
      UPDATE bills SET latest_text_version_id = NULL;
      DELETE FROM bill_text_versions;
      DELETE FROM bill_cosponsor_history;
      DELETE FROM bill_subject_history;
      DELETE FROM bill_history;
      DELETE FROM bill_cosponsors;
      DELETE FROM bill_subjects;
      DELETE FROM bills;
    """.update.run.transact(xa).unsafeRunSync()
  }

}
