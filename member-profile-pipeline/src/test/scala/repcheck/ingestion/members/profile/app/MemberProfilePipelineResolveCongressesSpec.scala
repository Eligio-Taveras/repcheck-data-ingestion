package repcheck.ingestion.members.profile.app

import scala.concurrent.duration._

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.syntax.all._

import doobie._
import doobie.implicits._

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import repcheck.ingestion.common.api.CongressGovClientConfig
import repcheck.ingestion.common.db.DatabaseConfig
import repcheck.ingestion.common.events.EventPublisherConfig
import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}
import repcheck.ingestion.members.profile.app.MemberProfilePipeline.AppConfig
import repcheck.ingestion.members.profile.config.MemberProfileConfig
import repcheck.pipeline.models.errors.RetryConfig

/**
 * Direct coverage for [[MemberProfilePipeline.resolveCongresses]] across all three resolution layers (env var, config
 * field, DB-derived). Uses an injectable `envGetter` so the env-path is exercised without mutating the JVM environment,
 * and an in-memory H2 database so the DB-derived path can be exercised without DockerPostgres.
 */
class MemberProfilePipelineResolveCongressesSpec extends AnyFlatSpec with Matchers {

  private val testRetry = RetryConfig(
    maxRetries = 0,
    initialBackoffMs = 1,
    maxBackoffMs = 10,
    backoffMultiplier = 1.0,
  )

  private val baseConfig = AppConfig(
    database = DatabaseConfig(
      host = "localhost",
      port = 5432,
      database = "repcheck_test",
      username = "repcheck",
      password = "repcheck",
      maxConnections = 2,
    ),
    congressApi = CongressGovClientConfig(
      apiKey = "test-api-key",
      baseUrl = "http://localhost:8080",
      retry = testRetry,
    ),
    pipeline = MemberProfileConfig(
      congresses = Nil,
      parallelism = 1,
      pageDelay = 0.millis,
      eventPublishRetry = testRetry,
    ),
    eventPublisher = EventPublisherConfig(
      projectId = "repcheck-test",
      topicName = "test-member-events",
      source = "member-profile-pipeline-test",
    ),
  )

  /**
   * Stub logger that no-ops every call. The resolver only uses `info`; the others are required by the trait but should
   * never be invoked under happy-path coverage. Per the project's no-null-in-tests rule, we return `IO.unit` directly
   * rather than using a mock with an `Option[Throwable]` matcher.
   */
  private class StubLogger extends PipelineLogger[IO] {
    override def info(context: LogContext, message: String): IO[Unit]                            = IO.unit
    override def warn(context: LogContext, message: String): IO[Unit]                            = IO.unit
    override def error(context: LogContext, message: String, cause: Option[Throwable]): IO[Unit] = IO.unit
    override def debug(context: LogContext, message: String): IO[Unit]                           = IO.unit
  }

  // Each test gets its own H2 instance so they don't see each other's seeded rows.
  private def freshXa(name: String): Transactor[IO] =
    Transactor.fromDriverManager[IO](
      driver = "org.h2.Driver",
      url = s"jdbc:h2:mem:resolveCongresses_$name;DB_CLOSE_DELAY=-1",
      user = "",
      password = "",
      logHandler = None,
    )

  private def createBillsTable(xa: Transactor[IO]): IO[Unit] =
    sql"""
      CREATE TABLE bills (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        congress INT
      )
    """.update.run.transact(xa).void

  private def insertBills(xa: Transactor[IO], congresses: List[Int]): IO[Unit] = {
    val inserts = congresses
      .map(c => sql"INSERT INTO bills (congress) VALUES ($c)".update.run)
      .reduceOption(_ *> _)
      .getOrElse(doobie.free.connection.unit)
    inserts.transact(xa).void
  }

  "resolveCongresses" should "use the env var when MEMBERS_CONGRESSES is set, ignoring config and DB" in {
    val xa     = freshXa("env_path")
    val logger = new StubLogger

    // Config has a list (would normally be used) and DB has no bills table — both irrelevant when env wins.
    val config = baseConfig.copy(pipeline = baseConfig.pipeline.copy(congresses = List(99, 98)))

    val envGetter: String => Option[String] = {
      case "MEMBERS_CONGRESSES" => Some("117,118,119")
      case _                    => None
    }

    val result = MemberProfilePipeline.resolveCongresses[IO](config, xa, logger, envGetter).unsafeRunSync()
    result shouldBe List(117, 118, 119)
  }

  it should "trim whitespace and skip empty tokens in MEMBERS_CONGRESSES" in {
    val xa     = freshXa("env_trim")
    val logger = new StubLogger
    val envGetter: String => Option[String] = {
      case "MEMBERS_CONGRESSES" => Some("  117 , 118 , , 119  ")
      case _                    => None
    }

    val result = MemberProfilePipeline.resolveCongresses[IO](baseConfig, xa, logger, envGetter).unsafeRunSync()
    result shouldBe List(117, 118, 119)
  }

  it should "treat an empty MEMBERS_CONGRESSES env var as if unset (falls through to next layer)" in {
    val xa     = freshXa("env_empty")
    val logger = new StubLogger
    // Empty / whitespace-only env should fall through to config; populate config so we know it was reached.
    val config = baseConfig.copy(pipeline = baseConfig.pipeline.copy(congresses = List(118, 119)))
    val envGetter: String => Option[String] = {
      case "MEMBERS_CONGRESSES" => Some("   ")
      case _                    => None
    }

    val result = MemberProfilePipeline.resolveCongresses[IO](config, xa, logger, envGetter).unsafeRunSync()
    result shouldBe List(118, 119)
  }

  it should "use config.pipeline.congresses when env is unset and config is non-empty" in {
    val xa     = freshXa("config_path")
    val logger = new StubLogger
    val config = baseConfig.copy(pipeline = baseConfig.pipeline.copy(congresses = List(118, 119)))

    val result = MemberProfilePipeline.resolveCongresses[IO](config, xa, logger, _ => None).unsafeRunSync()
    result shouldBe List(118, 119)
  }

  it should "fall back to the bills table when both env and config are empty" in {
    val xa     = freshXa("db_path")
    val logger = new StubLogger

    (createBillsTable(xa) *> insertBills(xa, List(118, 117, 118, 119, 116))).unsafeRunSync()

    val result = MemberProfilePipeline.resolveCongresses[IO](baseConfig, xa, logger, _ => None).unsafeRunSync()
    // DISTINCT + ORDER BY congress DESC should yield each congress once, newest first.
    result shouldBe List(119, 118, 117, 116)
  }

  it should "return an empty list when DB-derived path finds no bills" in {
    val xa     = freshXa("db_empty")
    val logger = new StubLogger

    createBillsTable(xa).unsafeRunSync()
    // No bills inserted.

    val result = MemberProfilePipeline.resolveCongresses[IO](baseConfig, xa, logger, _ => None).unsafeRunSync()
    result shouldBe empty
  }

  it should "skip congress=NULL rows in the DB-derived path" in {
    val xa     = freshXa("db_nulls")
    val logger = new StubLogger

    val seed =
      sql"""
        CREATE TABLE bills (
          id BIGINT AUTO_INCREMENT PRIMARY KEY,
          congress INT
        )
      """.update.run *>
        sql"INSERT INTO bills (congress) VALUES (118)".update.run *>
        sql"INSERT INTO bills (congress) VALUES (NULL)".update.run *>
        sql"INSERT INTO bills (congress) VALUES (119)".update.run

    val _ = seed.transact(xa).unsafeRunSync()

    val result = MemberProfilePipeline.resolveCongresses[IO](baseConfig, xa, logger, _ => None).unsafeRunSync()
    result shouldBe List(119, 118)
  }

}
