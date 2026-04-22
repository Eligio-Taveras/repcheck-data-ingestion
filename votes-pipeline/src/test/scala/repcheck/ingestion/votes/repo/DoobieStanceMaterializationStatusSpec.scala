package repcheck.ingestion.votes.repo

import java.time.Instant

import cats.effect.unsafe.implicits.global

import doobie.implicits._
import doobie.postgres.implicits._

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import repcheck.ingestion.votes.testing.TransactorFixture
import repcheck.members.common.testing.DockerRequired

/**
 * Integration tests for [[DoobieStanceMaterializationStatusRepository]] against DockerPostgres. Supports §6.5 AC rows
 * 29-34 by proving `markHasVotes` is an idempotent upsert that advances `votes_updated_at` without clobbering the
 * analysis-pipeline-owned flags.
 */
class DoobieStanceMaterializationStatusSpec extends AnyFlatSpec with Matchers with TransactorFixture {

  private lazy val repo = new DoobieStanceMaterializationStatusRepository

  private def loadRow(billId: Long): (Boolean, Boolean, Boolean, Option[Instant], Option[Instant], Option[Instant]) =
    sql"""SELECT has_votes, has_analysis, all_passes_completed, votes_updated_at, analysis_completed_at, stances_materialized_at
          FROM stance_materialization_status WHERE bill_id = $billId"""
      .query[(Boolean, Boolean, Boolean, Option[Instant], Option[Instant], Option[Instant])]
      .unique
      .transact(xa)
      .unsafeRunSync()

  "markHasVotes" should "insert a row with has_votes = TRUE when the bill has no tracker row yet" taggedAs DockerRequired in {
    val billId = insertBill(billNumber = 400)
    repo.markHasVotes(billId).transact(xa).unsafeRunSync()

    val (hasVotes, hasAnalysis, allPasses, votesTs, analysisTs, stancesTs) = loadRow(billId)
    val _                                                                  = hasVotes shouldBe true
    val _                                                                  = hasAnalysis shouldBe false
    val _                                                                  = allPasses shouldBe false
    val _                                                                  = votesTs.isDefined shouldBe true
    val _                                                                  = analysisTs shouldBe None
    stancesTs shouldBe None
  }

  it should "be idempotent on subsequent calls (upsert)" taggedAs DockerRequired in {
    val billId = insertBill(billNumber = 401)
    repo.markHasVotes(billId).transact(xa).unsafeRunSync()
    repo.markHasVotes(billId).transact(xa).unsafeRunSync()
    repo.markHasVotes(billId).transact(xa).unsafeRunSync()

    val count =
      sql"SELECT COUNT(*) FROM stance_materialization_status WHERE bill_id = $billId"
        .query[Long]
        .unique
        .transact(xa)
        .unsafeRunSync()
    count shouldBe 1L
  }

  it should "advance votes_updated_at on repeated calls" taggedAs DockerRequired in {
    val billId = insertBill(billNumber = 402)
    repo.markHasVotes(billId).transact(xa).unsafeRunSync()
    val (_, _, _, firstTs, _, _) = loadRow(billId)
    Thread.sleep(10)
    repo.markHasVotes(billId).transact(xa).unsafeRunSync()
    val (_, _, _, secondTs, _, _) = loadRow(billId)

    val firstInstant  = firstTs.getOrElse(Instant.EPOCH)
    val secondInstant = secondTs.getOrElse(Instant.EPOCH)
    secondInstant.isAfter(firstInstant) shouldBe true
  }

  it should "not clobber analysis-owned columns on conflict" taggedAs DockerRequired in {
    val billId          = insertBill(billNumber = 403)
    val analysisInstant = Instant.parse("2024-02-01T00:00:00Z")
    // Simulate the analysis pipeline having already set has_analysis, analysis_completed_at, and all_passes_completed.
    val _ = sql"""INSERT INTO stance_materialization_status
                  (bill_id, has_votes, has_analysis, all_passes_completed, analysis_completed_at)
                  VALUES ($billId, FALSE, TRUE, TRUE, $analysisInstant)""".update.run.transact(xa).unsafeRunSync()

    repo.markHasVotes(billId).transact(xa).unsafeRunSync()

    val (hasVotes, hasAnalysis, allPasses, _, analysisTs, _) = loadRow(billId)
    val _                                                    = hasVotes shouldBe true
    val _                                                    = hasAnalysis shouldBe true
    val _                                                    = allPasses shouldBe true
    analysisTs shouldBe Some(analysisInstant)
  }

}
