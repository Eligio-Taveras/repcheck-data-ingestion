package repcheck.members.lismapping.testing

import cats.effect.IO

import doobie.Transactor
import doobie.implicits._

import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Suite}

/**
 * Provides a shared AlloyDB Omni container and Doobie transactor for Docker-backed integration tests in the
 * lis-mapping-refresher project. Suites share one container (via [[SharedDockerPostgres]]) and run sequentially (`Test
 * / parallelExecution := false` in build.sbt) to avoid cross-suite FK violations during cleanup.
 *
 * Seeds placeholder `members` rows so FK constraints on `member_lis_mapping.member_id` are satisfied. Cleans
 * `member_lis_mapping` and `lis_members` after every test to isolate state.
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
   * Insert placeholder member rows so FK constraints on `member_lis_mapping.member_id` are satisfied. Uses `ON CONFLICT
   * DO NOTHING` to be idempotent across tests.
   */
  private def seedMembers(): Unit = {
    val _ = sql"""
      INSERT INTO members (natural_key) VALUES ('LIS-M001')
        ON CONFLICT (natural_key) DO NOTHING;
      INSERT INTO members (natural_key) VALUES ('LIS-M002')
        ON CONFLICT (natural_key) DO NOTHING;
      INSERT INTO members (natural_key) VALUES ('LIS-M003')
        ON CONFLICT (natural_key) DO NOTHING;
      INSERT INTO members (natural_key) VALUES ('LIS-M004')
        ON CONFLICT (natural_key) DO NOTHING;
      INSERT INTO members (natural_key) VALUES ('LIS-M005')
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
      DELETE FROM member_lis_mapping;
      DELETE FROM lis_members;
    """.update.run.transact(xa).unsafeRunSync()
  }

}
