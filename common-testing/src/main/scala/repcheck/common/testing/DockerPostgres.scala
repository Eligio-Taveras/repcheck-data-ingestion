package repcheck.common.testing

import java.sql.{Connection, DriverManager}

import scala.annotation.tailrec
import scala.sys.process._
import scala.util.Try

import cats.effect.{IO, Resource}

import repcheck.db.migrations.MigrationRunner

/**
 * Shared Docker-backed Postgres test fixture for any sub-project's `% Test` config.
 *
 * History: this file was duplicated across `bills-common/src/test/.../testing/DockerPostgres.scala` and
 * `members-common/src/test/.../testing/DockerPostgres.scala`. The two copies were 99% identical (only `dbName`,
 * `containerName` prefix, and package differed) and drifted whenever one side got a fix the other didn't (e.g., the
 * Windows portability fix in PR #106 took two passes — once for bills-common, then a discovery + repeat for
 * members-common when round-trip tests for the member side hit the same `CreateProcess error=2`). Consolidating into a
 * single source removes that drift surface.
 *
 * Schema neutrality: this fixture spins up postgres and applies the FULL Liquibase changelog (every migration this
 * runner version knows about), so the resulting DB has bills, members, votes, etc. tables regardless of which sub-
 * project's tests use it. The bill / member specifics are layered on top via `TransactorFixture` traits in each common
 * module — those keep their per-module concerns (different `cleanTables` lists, different seeders).
 */
final case class PostgresContainerInfo(jdbcUrl: String, user: String, password: String) {

  def getConnection: Connection =
    DriverManager.getConnection(jdbcUrl, user, password)

}

object DockerPostgres {

  private val dbName: String          = "repcheck_test"
  private val dbUser: String          = "test"
  private val dbPassword: String      = "test"
  private val image: String           = "google/alloydbomni:16.8.0"
  private val maxReadyAttempts: Int   = 120
  private val readyDelayMs: Long      = 1000L
  private val maxConnectAttempts: Int = 60
  private val connectDelayMs: Long    = 1000L

  // On Windows, Java's `ProcessBuilder.start` (used by `scala.sys.process`) does NOT auto-resolve PATHEXT extensions
  // AND the sbt-forked JVM child's search PATH is not always the same as the launching bash's. `Seq("docker", ...).!`
  // resolves to the bash wrapper that CreateProcess can't execute. Portable fix: respect a `DOCKER_BIN` env var
  // override, fall back to the Windows-default Docker Desktop absolute path, fall back to bare `docker` on Linux/macOS.
  // CI on Linux picks `docker` unchanged. Pure logic extracted into `resolveDockerBin` for testability.
  private val dockerBin: String =
    resolveDockerBin(sys.props.get("os.name"), sys.env.get("DOCKER_BIN"))

  private[testing] def resolveDockerBin(osName: Option[String], envOverride: Option[String]): String =
    envOverride.getOrElse {
      val isWindows = osName.exists(_.toLowerCase.contains("windows"))
      if (isWindows) """C:\Program Files\Docker\Docker\resources\bin\docker.exe""" else "docker"
    }

  /** Parse `docker port <name> 5432` output (e.g., `0.0.0.0:54321`) into the host port. */
  private[testing] def parseHostPort(portOutput: String): Int =
    portOutput.trim
      .split(':')
      .lastOption
      .getOrElse(sys.error(s"Unexpected docker port output: $portOutput"))
      .toInt

  // Error-branch helpers extracted from the IO methods so the failure paths can be unit-tested without
  // standing up a container or mocking docker. Each is invoked exactly once from its host method below;
  // the inline code is the same as before but factored out so codecov can register both branches via the
  // unit suite. Without these the failure lines stayed at 0 invocations on PR #107.

  private[testing] def requireContainerStartSucceeded(exitCode: Int): Unit =
    if (exitCode != 0) {
      sys.error("Failed to start Docker container. Is Docker running?")
    }

  private[testing] def failOnReadinessExhaustion(cleanup: () => Unit, maxAttempts: Int): Nothing = {
    cleanup()
    sys.error(s"PostgreSQL container did not become ready after $maxAttempts attempts")
  }

  final private case class ContainerHandle(name: String, info: PostgresContainerInfo)

  val resource: Resource[IO, PostgresContainerInfo] =
    Resource.make(acquire)(release).map(_.info)

  private def acquire: IO[ContainerHandle] = IO.blocking {
    val containerName = s"repcheck-test-${java.util.UUID.randomUUID().toString.take(8)}"
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

    requireContainerStartSucceeded(exitCode)

    parseHostPort(Seq(dockerBin, "port", containerName, "5432").!!)
  }

  @tailrec
  private def waitForReady(containerName: String, remaining: Int = maxReadyAttempts): Unit = {
    if (remaining <= 0) {
      failOnReadinessExhaustion(
        cleanup = () => { val _ = Seq(dockerBin, "rm", "-f", containerName).!; () },
        maxAttempts = maxReadyAttempts,
      )
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

  // Scoped `private[testing]` so the failure-branch unit suite can exercise both the retry path
  // (`Failure(_) if remaining > 1`) and the exhaustion path (`Failure(ex) =>`) without standing up a
  // container — passing a bad port fails the JDBC connect immediately and forces the desired branch.
  // Without test access these branches stayed at 0 invocations on codecov; reaching them via a real
  // container requires inducing a Postgres-side failure mid-startup, which is fragile.
  @tailrec
  private[testing] def connectWithRetry(port: Int, remaining: Int): Connection = {
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

/**
 * ScalaTest tag for tests that require a real Postgres container. Run via the `dockerTest` sbt alias (see build.sbt) or
 * filter explicitly with `-n DockerRequired`. Default test runs filter these out via `-l DockerRequired`.
 */
object DockerRequired extends org.scalatest.Tag("DockerRequired")

object E2ETest extends org.scalatest.Tag("com.repcheck.tags.E2ETest")
