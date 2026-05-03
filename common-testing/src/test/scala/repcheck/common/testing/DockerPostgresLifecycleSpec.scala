package repcheck.common.testing

import cats.effect.IO
import cats.effect.unsafe.implicits.global

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * Docker-tagged smoke test that drives `DockerPostgres.resource` through its full lifecycle (acquire → migrate → use →
 * release) once. Pulls coverage on the IO-bound paths (`startContainer`, `waitForReady`, `applyMigrations`,
 * `connectWithRetry`, `release`) into commonTesting's own scoverage report — without this, those paths are exercised
 * only by DockerRequired specs in `billsCommon` / `membersCommon` / pipelines, where the hits land in those
 * subprojects' scoverage data and never roll up into commonTesting's `scoverage.xml`. Codecov reads each subproject's
 * report independently, so commonTesting's per-file patch coverage stayed at ~85% (just the pure helpers) until this
 * test landed.
 *
 * The assertion is intentionally minimal: a `SELECT 1` round-trip proves Postgres is reachable through the JDBC URL the
 * resource produced and that Liquibase migrations didn't leave the DB in a broken state. The full per-pipeline schema
 * assertions live in their own DockerRequired suites and don't belong here — this is a wiring test.
 */
class DockerPostgresLifecycleSpec extends AnyFlatSpec with Matchers {

  "DockerPostgres.resource" should "spin up Postgres, apply migrations, and tear down cleanly" taggedAs DockerRequired in {
    val program = DockerPostgres.resource.use { info =>
      IO.blocking {
        val conn = info.getConnection
        try {
          val stmt = conn.createStatement()
          try {
            val rs = stmt.executeQuery("SELECT 1")
            try {
              val _ = rs.next() shouldBe true
              rs.getInt(1) shouldBe 1
            } finally rs.close()
          } finally stmt.close()
        } finally conn.close()
      }
    }
    program.unsafeRunSync()
  }

}
