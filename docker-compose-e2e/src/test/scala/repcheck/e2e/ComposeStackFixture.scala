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

  /**
   * Run the amendments slice of the e2e stack on top of an already-started infra. Intended for callers that have called
   * [[start]] (or [[startInfraOnly]]) first. Sequence:
   *
   *   1. `amendments-pipeline` (one-shot) — drains the WireMock /v3/amendment list and persists every fixture amendment
   *      + its inline-recursed parents. 2. `amendment-text-pipeline` (long-running, started detached) — subscriber for
   *      `amendment-text-available`. Started BEFORE the checker so an event published during step 3 is delivered live
   *      rather than queued indefinitely. 3. `amendment-text-availability-checker` (one-shot) — polls candidate rows in
   *      `amendments`, fetches text-version metadata from WireMock, publishes `AmendmentTextAvailableEvent` for each
   *      new (versionType, formatType) tuple. 4. Wait for `amendment-text-pipeline` to drain — best-effort polling on
   *      the side-effect (`amendment_text_versions` row count) in the caller's spec; this method just makes sure the
   *      container is up.
   *
   * The amendment-text-pipeline container is left running and torn down by [[stop]].
   */
  def startAmendments(): Unit = {
    runOneShot("amendments-pipeline")
    // amendment-text-pipeline is a long-running Cloud Run Service — bring it up
    // detached so the checker's published event arrives at a live subscriber.
    upBackground("amendment-text-pipeline")
    runOneShot("amendment-text-availability-checker")
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

  /**
   * Start a long-running service detached. Unlike [[runOneShot]] this does NOT block on container exit — useful for the
   * amendment-text-pipeline subscriber which needs to be live while a separate one-shot publishes events into it.
   * Caller-side polling (DB-row appearance, container log scrape) confirms readiness; this method only guarantees the
   * container has been requested.
   */
  private def upBackground(service: String): Unit = {
    val exit = compose("up", "-d", "--no-deps", service)
    if (exit != 0) {
      sys.error(s"`docker compose up -d $service` exited with code $exit")
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
