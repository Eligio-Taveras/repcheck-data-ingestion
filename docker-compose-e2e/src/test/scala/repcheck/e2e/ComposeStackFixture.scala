package repcheck.e2e

import java.util.UUID
import java.util.concurrent.atomic.AtomicBoolean

import scala.annotation.tailrec
import scala.sys.process._
import scala.util.control.NonFatal

/**
 * Lifecycle manager for the `docker-compose.e2e.yml` stack. Follows the in-house
 * [[repcheck.ingestion.bills.common.testing.DockerPostgres]] pattern — `scala.sys.process._` for docker CLI
 * invocations, blocking semantics, and a JVM-wide shutdown hook so a test-abort leaves no orphan containers.
 *
 * Usage:
 * {{{
 *   val fixture = new ComposeStackFixture()
 *   fixture.start()  // blocks until infra healthy + every pipeline has run + exited 0
 *   try {
 *     // assertions against localhost:5432 (alloydb) + localhost:8085 (pubsub)
 *   } finally {
 *     fixture.stop()
 *   }
 * }}}
 *
 * Every invocation is namespaced under a unique `composeProject` so parallel CI jobs or dev worktrees never collide on
 * container/network/volume names.
 */
final class ComposeStackFixture(
  val composeProject: String = s"e2e-stack-${UUID.randomUUID().toString.take(8)}",
  val composeFile: String = "docker-compose.e2e.yml",
  val healthTimeoutMs: Long = 120_000L,
  val healthPollMs: Long = 2_000L,
) {

  private val stopped = new AtomicBoolean(false)

  // Register a shutdown hook up-front so a process kill (Ctrl-C, sbt abort)
  // still tears down our containers. Idempotent via the `stopped` flag.
  Runtime.getRuntime.addShutdownHook(new Thread(() => silentStop()))

  def start(): Unit = {
    upInfra()
    waitForHealth("alloydb")
    waitForHealth("pubsub-emulator")
    waitForHealth("wiremock")
    runOneShot("db-migrations")
    runOneShot("pubsub-init")
    // member-profile-pipeline FIRST — it populates the `members` table that
    // votes-pipeline's House positions FK into (via bioguide-resolved
    // `vote_positions.member_id`). lis-mapping-refresher runs next so
    // `member_lis_mapping` exists before any Senate vote's position is
    // attributed. Bills still run ahead of votes because votes-pipeline
    // links votes to their bills via natural key.
    runOneShot("member-profile-pipeline")
    runOneShot("lis-mapping-refresher")
    runOneShot("bill-metadata-pipeline")
    runOneShot("bill-text-availability-checker")
    runOneShot("bill-text-pipeline")
    runOneShot("votes-pipeline")
  }

  def stop(): Unit =
    if (!stopped.getAndSet(true)) {
      val _ = compose("down", "-v", "--remove-orphans")
    }

  private def silentStop(): Unit =
    try {
      val _ = stop()
    } catch {
      case NonFatal(_) => ()
    }

  /**
   * The host-side JDBC URL for the alloydb service. The compose file publishes port 5432, so host-side connections work
   * without any `docker exec` dance.
   */
  def alloydbJdbcUrl: String  = "jdbc:postgresql://localhost:5432/repcheck_e2e?sslmode=disable"
  def alloydbUser: String     = "repcheck"
  def alloydbPassword: String = "repcheck"

  /** Host-side Pub/Sub emulator REST base URL. Compose publishes port 8085. */
  def pubsubEmulatorHost: String = "http://localhost:8085"
  def pubsubProjectId: String    = "repcheck-e2e"

  // ---------------------------------------------------------------------------
  // Internals
  // ---------------------------------------------------------------------------

  /**
   * Run a `docker compose` subcommand in the project namespace. Streams stdout + stderr to the current process so sbt
   * test logs show exactly what docker saw, matching the DockerPostgres fixture's debugability trade-off.
   */
  private def compose(args: String*): Int = {
    val cmd = Seq("docker", "compose", "-p", composeProject, "-f", composeFile) ++ args
    Process(cmd).!
  }

  private def upInfra(): Unit = {
    val exit = compose("up", "-d", "--build", "alloydb", "pubsub-emulator", "wiremock")
    if (exit != 0) {
      sys.error(s"`docker compose up` for infra services exited with code $exit")
    }
  }

  private def runOneShot(service: String): Unit = {
    val exit = compose("run", "--rm", "--no-deps", service)
    if (exit != 0) {
      sys.error(s"`docker compose run $service` exited with code $exit")
    }
  }

  @tailrec
  private def waitForHealth(service: String, elapsedMs: Long = 0L): Unit = {
    val container = s"$composeProject-$service-1"
    val status =
      Process(Seq("docker", "inspect", "--format", "{{.State.Health.Status}}", container)).lazyLines_!.headOption
        .getOrElse("")
    if (status == "healthy") {
      ()
    } else if (elapsedMs >= healthTimeoutMs) {
      sys.error(s"Service '$service' did not become healthy within ${healthTimeoutMs}ms (last status: '$status')")
    } else {
      Thread.sleep(healthPollMs)
      waitForHealth(service, elapsedMs + healthPollMs)
    }
  }

}
