package repcheck.ingestion.bills.summary.persistence

import java.time.Instant

import cats.effect.IO
import cats.effect.unsafe.implicits.global

import doobie.implicits._
import doobie.postgres.implicits._

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import repcheck.ingestion.bills.common.testing.{DockerRequired, TransactorFixture}

/**
 * Round-trip integration tests for [[DoobieWorkflowRunStepsRepository]] against a real AlloyDB Omni container.
 *
 * Reuses [[TransactorFixture]] from bills-common — same container, same `xa` transactor. We don't need any of the
 * fixture's per-test seeding (its `seedMembers` writes to the members table; we only touch workflow_runs and
 * workflow_run_steps), but the cleanup helper happens to leave our two tables alone, so it's safe to inherit.
 *
 * Each test seeds a workflow_runs parent + N workflow_run_steps rows, runs the query, and asserts on the result.
 * Cleanup is per-test in the `afterEach` overrides below — we delete only the rows we own (keyed by step_name) so the
 * tests are independent.
 */
class DoobieWorkflowRunStepsRepositorySpec extends AnyFlatSpec with Matchers with TransactorFixture {

  private lazy val repo = new DoobieWorkflowRunStepsRepository

  /** Insert a workflow_runs parent and return its BIGSERIAL id. */
  private def insertWorkflowRun(): Long =
    sql"""INSERT INTO workflow_runs (workflow_name, status, started_at)
          VALUES ('test-workflow', 'completed'::workflow_status_type, NOW())
          RETURNING id""".query[Long].unique.transact(xa).unsafeRunSync()

  /**
   * Insert a workflow_run_steps row. `status` is bound as a String and cast to the enum type at the SQL site to keep
   * the test fixture independent of any Scala-side enum mapping.
   */
  private def insertStep(
    workflowRunId: Long,
    stepName: String,
    status: String,
    startedAt: Option[Instant],
    completedAt: Option[Instant],
  ): Long = {
    val statusFr = doobie.Fragment.const(s"'$status'::workflow_status_type")
    sql"""INSERT INTO workflow_run_steps (workflow_run_id, step_name, status, started_at, completed_at)
          VALUES ($workflowRunId, $stepName, $statusFr, $startedAt, $completedAt)
          RETURNING id""".query[Long].unique.transact(xa).unsafeRunSync()
  }

  private def cleanStepsByName(stepName: String): Unit = {
    val _ = sql"DELETE FROM workflow_run_steps WHERE step_name = $stepName".update.run.transact(xa).unsafeRunSync()
  }

  "lastSuccessfulEndAt" should "return None when no row exists for the step" taggedAs DockerRequired in {
    repo.lastSuccessfulEndAt("never-ran").transact(xa).unsafeRunSync() shouldBe None
  }

  it should "return None when the only row for the step is still 'running'" taggedAs DockerRequired in {
    val stepName = "test-step-running-only"
    val runId    = insertWorkflowRun()
    val _        = insertStep(runId, stepName, "running", Some(Instant.now()), completedAt = None)

    val _ = repo.lastSuccessfulEndAt(stepName).transact(xa).unsafeRunSync() shouldBe None
    cleanStepsByName(stepName)
  }

  it should "return None when the only completed row has NULL completed_at (defensive)" taggedAs DockerRequired in {
    val stepName = "test-step-completed-no-timestamp"
    val runId    = insertWorkflowRun()
    val _        = insertStep(runId, stepName, "completed", Some(Instant.now()), completedAt = None)

    val _ = repo.lastSuccessfulEndAt(stepName).transact(xa).unsafeRunSync() shouldBe None
    cleanStepsByName(stepName)
  }

  it should "return the completed_at of the only completed row" taggedAs DockerRequired in {
    val stepName    = "test-step-single-completed"
    val runId       = insertWorkflowRun()
    val completedAt = Instant.parse("2024-06-01T12:00:00Z")
    val _           = insertStep(runId, stepName, "completed", Some(completedAt.minusSeconds(60)), Some(completedAt))

    val result = repo.lastSuccessfulEndAt(stepName).transact(xa).unsafeRunSync()
    val _      = result shouldBe defined
    // PostgreSQL TIMESTAMPTZ → java.time.Instant round-trip preserves the instant exactly.
    val _ = result.map(_.toEpochMilli) shouldBe Some(completedAt.toEpochMilli)
    cleanStepsByName(stepName)
  }

  it should "return MAX(completed_at) when multiple completed rows exist" taggedAs DockerRequired in {
    val stepName = "test-step-multiple-completed"
    val runId    = insertWorkflowRun()
    val first    = Instant.parse("2024-01-01T00:00:00Z")
    val middle   = Instant.parse("2024-06-01T00:00:00Z")
    val last     = Instant.parse("2024-12-01T00:00:00Z")

    val _ = insertStep(runId, stepName, "completed", Some(first.minusSeconds(60)), Some(first))
    val _ = insertStep(runId, stepName, "completed", Some(last.minusSeconds(60)), Some(last))
    val _ = insertStep(runId, stepName, "completed", Some(middle.minusSeconds(60)), Some(middle))

    val result = repo.lastSuccessfulEndAt(stepName).transact(xa).unsafeRunSync()
    val _      = result.map(_.toEpochMilli) shouldBe Some(last.toEpochMilli)
    cleanStepsByName(stepName)
  }

  it should "include 'completed_with_errors' rows in the watermark (per the migration-017a enum)" taggedAs DockerRequired in {
    val stepName  = "test-step-completed-with-errors"
    val runId     = insertWorkflowRun()
    val withErrAt = Instant.parse("2024-09-01T12:00:00Z")
    val _ = insertStep(runId, stepName, "completed_with_errors", Some(withErrAt.minusSeconds(60)), Some(withErrAt))

    // A run that finished but had per-item failures still establishes a watermark — the next run only needs
    // to look back as far as the previous run's wall-clock end time, regardless of its per-item success rate.
    val result = repo.lastSuccessfulEndAt(stepName).transact(xa).unsafeRunSync()
    val _      = result.map(_.toEpochMilli) shouldBe Some(withErrAt.toEpochMilli)
    cleanStepsByName(stepName)
  }

  it should "exclude 'failed' rows" taggedAs DockerRequired in {
    val stepName = "test-step-failed"
    val runId    = insertWorkflowRun()
    val failedAt = Instant.parse("2024-08-01T12:00:00Z")
    val _        = insertStep(runId, stepName, "failed", Some(failedAt.minusSeconds(60)), Some(failedAt))

    val _ = repo.lastSuccessfulEndAt(stepName).transact(xa).unsafeRunSync() shouldBe None
    cleanStepsByName(stepName)
  }

  it should "scope to the supplied step_name (other steps don't pollute the watermark)" taggedAs DockerRequired in {
    val ourStep   = "test-step-scoped-ours"
    val otherStep = "test-step-scoped-other"
    val runId     = insertWorkflowRun()
    val ourTime   = Instant.parse("2024-03-01T00:00:00Z")
    val otherTime = Instant.parse("2024-12-31T00:00:00Z")

    val _ = insertStep(runId, ourStep, "completed", Some(ourTime.minusSeconds(60)), Some(ourTime))
    val _ = insertStep(runId, otherStep, "completed", Some(otherTime.minusSeconds(60)), Some(otherTime))

    val result = repo.lastSuccessfulEndAt(ourStep).transact(xa).unsafeRunSync()
    val _      = result.map(_.toEpochMilli) shouldBe Some(ourTime.toEpochMilli)
    cleanStepsByName(ourStep)
    cleanStepsByName(otherStep)
  }

}
