package repcheck.ingestion.amendments.testing

import java.sql.{Connection, DriverManager}

import scala.annotation.tailrec
import scala.sys.process._
import scala.util.Try

import cats.effect.{IO, Resource}

import repcheck.db.migrations.MigrationRunner

/**
 * Connection details for the test AlloyDB Omni container. Returned by [[DockerPostgres.resource]]; suite traits read
 * `jdbcUrl`/`user`/`password` to construct the Doobie transactor.
 */
final case class PostgresContainerInfo(jdbcUrl: String, user: String, password: String) {

  def getConnection: Connection =
    DriverManager.getConnection(jdbcUrl, user, password)

}

/**
 * Spawns a single AlloyDB Omni container for the amendments-pipeline test runs and applies every migration. Mirrors the
 * bills-common / members-common implementations so the local-dev experience and CI surface stay uniform across
 * subprojects.
 */
object DockerPostgres {

  private val dbName: String          = "repcheck_amendments_test"
  private val dbUser: String          = "test"
  private val dbPassword: String      = "test"
  private val image: String           = "google/alloydbomni:16.8.0"
  private val maxReadyAttempts: Int   = 120
  private val readyDelayMs: Long      = 1000L
  private val maxConnectAttempts: Int = 60
  private val connectDelayMs: Long    = 1000L

  // On Windows, Java's `ProcessBuilder.start` (used under the hood by `scala.sys.process`) does not auto-resolve
  // PATHEXT extensions. The same `DOCKER_BIN` override that bills-common uses keeps Docker discovery portable across
  // sbt's forked-JVM launch path on Windows and CI on Linux.
  private val dockerBin: String = {
    val isWindows = sys.props.get("os.name").exists(_.toLowerCase.contains("windows"))
    sys.env.getOrElse(
      "DOCKER_BIN",
      if (isWindows) """C:\Program Files\Docker\Docker\resources\bin\docker.exe""" else "docker",
    )
  }

  final private case class ContainerHandle(name: String, info: PostgresContainerInfo)

  val resource: Resource[IO, PostgresContainerInfo] =
    Resource.make(acquire)(release).map(_.info)

  private def acquire: IO[ContainerHandle] = IO.blocking {
    val containerName = s"repcheck-amendments-test-${java.util.UUID.randomUUID().toString.take(8)}"
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
    val _ = Seq(dockerBin, "rm", "-f", handle.name).!
    ()
  }

  private def startContainer(containerName: String): Int = {
    val exitCode = Seq(
      dockerBin,
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

    val portOutput = Seq(dockerBin, "port", containerName, "5432").!!.trim
    portOutput
      .split(':')
      .lastOption
      .getOrElse(sys.error(s"Unexpected docker port output: $portOutput"))
      .toInt
  }

  @tailrec
  private def waitForReady(containerName: String, remaining: Int = maxReadyAttempts): Unit = {
    if (remaining <= 0) {
      val _ = Seq(dockerBin, "rm", "-f", containerName).!
      sys.error(s"PostgreSQL container did not become ready after ${maxReadyAttempts.toString} attempts")
    }

    val ready = Try {
      Seq(dockerBin, "exec", containerName, "pg_isready", "-U", dbUser, "-d", dbName).!!
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
        sys.error(s"Failed to connect to PostgreSQL after ${maxConnectAttempts.toString} attempts: ${ex.getMessage}")
    }
  }

}

/**
 * Lazy singleton wrapper around [[DockerPostgres.resource]]. Tests share one container per JVM run; the shutdown hook
 * tears it down after the JVM exits. Keeps the test wall-clock down because container startup dominates each suite's
 * runtime.
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

/** ScalaTest tag matching the global `DockerRequired` filter in `build.sbt`. Suites tag every Docker-backed test. */
object DockerRequired extends org.scalatest.Tag("DockerRequired")
