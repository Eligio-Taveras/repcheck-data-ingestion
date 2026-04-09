package com.repcheck.bills.common.testing

import cats.effect.IO

import doobie.Transactor
import doobie.implicits._

import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Suite}

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

  override def afterEach(): Unit = {
    cleanTables()
    super.afterEach()
  }

  private def cleanTables(): Unit = {
    val _ = sql"""
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
