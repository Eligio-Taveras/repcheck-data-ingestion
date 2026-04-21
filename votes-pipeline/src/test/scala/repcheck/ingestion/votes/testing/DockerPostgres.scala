package repcheck.ingestion.votes.testing

import java.sql.{Connection, DriverManager}

import scala.annotation.tailrec
import scala.sys.process._
import scala.util.Try

import cats.effect.{IO, Resource}

import repcheck.db.migrations.MigrationRunner

/**
 * Lightweight wrapper over the JDBC connection details for an AlloyDB Omni container. Mirrors the bills-common and
 * members-common fixtures so votes-pipeline tests can use the same shape without pulling a test-scope dependency on
 * either sibling module.
 */
final case class PostgresContainerInfo(jdbcUrl: String, user: String, password: String) {

  def getConnection: Connection =
    DriverManager.getConnection(jdbcUrl, user, password)

}

/**
 * Docker-managed AlloyDB Omni lifecycle for votes-pipeline integration tests. Each allocation gets a unique container
 * name (via UUID prefix) and a random host port, so parallel cross-subproject test runs don't collide. Migrations are
 * applied automatically after the container is ready, using the `db-migrations-runner` published by
 * `repcheck-db-migrations`.
 */
object DockerPostgres {

  private val dbName: String          = "repcheck_votes_test"
  private val dbUser: String          = "test"
  private val dbPassword: String      = "test"
  private val image: String           = "google/alloydbomni:16.8.0"
  private val maxReadyAttempts: Int   = 120
  private val readyDelayMs: Long      = 1000L
  private val maxConnectAttempts: Int = 60
  private val connectDelayMs: Long    = 1000L

  final private case class ContainerHandle(name: String, info: PostgresContainerInfo)

  val resource: Resource[IO, PostgresContainerInfo] =
    Resource.make(acquire)(release).map(_.info)

  private def acquire: IO[ContainerHandle] = IO.blocking {
    val containerName = s"repcheck-votes-test-${java.util.UUID.randomUUID().toString.take(8)}"
    val port          = startContainer(containerName)
    waitForReady(containerName)
    applyMigrations(port)
    ContainerHandle(
      name = containerName,
      info = PostgresContainerInfo(
        jdbcUrl = s"jdbc:postgresql://localhost:$port/$dbName?sslmode=disable",
        user = dbUser,
        password = dbPassword,
      ),
    )
  }

  private def release(handle: ContainerHandle): IO[Unit] = IO.blocking {
    val _ = Seq("docker", "rm", "-f", handle.name).!
    ()
  }

  private def startContainer(containerName: String): Int = {
    val exitCode = Seq(
      "docker",
      "run",
      "-d",
      "--name",
      containerName,
      "-e",
      s"POSTGRES_DB=$dbName",
      "-e",
      s"POSTGRES_USER=$dbUser",
      "-e",
      s"POSTGRES_PASSWORD=$dbPassword",
      "-p",
      "0:5432",
      image,
    ).!

    if (exitCode != 0) {
      sys.error("Failed to start Docker container. Is Docker running?")
    }

    val portOutput = Seq("docker", "port", containerName, "5432").!!.trim
    portOutput
      .split(':')
      .lastOption
      .getOrElse(sys.error(s"Unexpected docker port output: $portOutput"))
      .toInt
  }

  @tailrec
  private def waitForReady(containerName: String, remaining: Int = maxReadyAttempts): Unit = {
    if (remaining <= 0) {
      val _ = Seq("docker", "rm", "-f", containerName).!
      sys.error(s"PostgreSQL container did not become ready after $maxReadyAttempts attempts")
    }

    val ready = Try {
      Seq("docker", "exec", containerName, "pg_isready", "-U", dbUser, "-d", dbName).!!
    }.isSuccess

    if (!ready) {
      Thread.sleep(readyDelayMs)
      waitForReady(containerName, remaining - 1)
    }
  }

  private def applyMigrations(port: Int): Unit = {
    val conn = connectWithRetry(port, maxConnectAttempts)
    try MigrationRunner.migrate(conn)
    finally conn.close()
  }

  @tailrec
  private def connectWithRetry(port: Int, remaining: Int): Connection = {
    val result = Try {
      DriverManager.getConnection(
        s"jdbc:postgresql://localhost:$port/$dbName?sslmode=disable",
        dbUser,
        dbPassword,
      )
    }
    result match {
      case scala.util.Success(conn) => conn
      case scala.util.Failure(_) if remaining > 1 =>
        Thread.sleep(connectDelayMs)
        connectWithRetry(port, remaining - 1)
      case scala.util.Failure(ex) =>
        sys.error(s"Failed to connect to PostgreSQL after $maxConnectAttempts attempts: ${ex.getMessage}")
    }
  }

}

/**
 * JVM-wide shared AlloyDB Omni container for votes-pipeline specs. The first spec to call `info` allocates a single
 * container and applies migrations; subsequent specs reuse it. A shutdown hook removes the container when the JVM exits
 * so we don't leak containers across sbt invocations.
 */
object SharedDockerPostgres {

  private lazy val handle: (PostgresContainerInfo, IO[Unit]) = {
    import cats.effect.unsafe.implicits.global
    val (info, finalizer) = DockerPostgres.resource.allocated.unsafeRunSync()
    val _ = sys.addShutdownHook {
      val _ = finalizer.attempt.unsafeRunSync()
      ()
    }
    (info, finalizer)
  }

  def info: PostgresContainerInfo = handle._1
}

object DockerRequired extends org.scalatest.Tag("DockerRequired")
object E2ETest        extends org.scalatest.Tag("com.repcheck.tags.E2ETest")
