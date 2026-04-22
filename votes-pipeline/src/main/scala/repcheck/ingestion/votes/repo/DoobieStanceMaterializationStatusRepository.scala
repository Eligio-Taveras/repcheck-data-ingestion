package repcheck.ingestion.votes.repo

import cats.syntax.all._

import doobie._
import doobie.implicits._

import repcheck.pipeline.models.constants.Tables

/**
 * Doobie implementation of [[StanceMaterializationStatusRepository]].
 *
 * ==Table purpose==
 *
 * `stance_materialization_status` (migration 009) is a per-bill gating table that tracks whether every prerequisite for
 * scoring is in place. The stance materializer and the scoring engine read these flags to decide whether a bill is
 * ready for the next step. It is **not** a per-member stance store — that data lives in `member_bill_stances`. Column
 * layout:
 *
 *   - `bill_id BIGINT UNIQUE` — one row per bill (migration 011 swapped the PK to a surrogate `id BIGSERIAL` and added
 *     `UNIQUE (bill_id)`).
 *   - `has_votes BOOLEAN` + `votes_updated_at TIMESTAMPTZ` — owned by votes-pipeline. Flipped to `TRUE` the first time
 *     a vote is recorded for the bill and refreshed on every subsequent vote.
 *   - `has_analysis BOOLEAN` + `analysis_completed_at TIMESTAMPTZ` — owned by the LLM bill-analysis pipeline.
 *   - `all_passes_completed BOOLEAN` + `stances_materialized_at TIMESTAMPTZ` — owned by the stance materializer; set
 *     when every required analysis pass has produced a stance for this bill.
 *   - `last_scoring_run_at TIMESTAMPTZ` — owned by the scoring engine.
 *
 * ==Our contract==
 *
 * The only operation the votes pipeline performs is `markHasVotes`: an INSERT with ON CONFLICT DO UPDATE keyed by the
 * `bill_id` UNIQUE constraint. We set `has_votes = TRUE` + `votes_updated_at = NOW()` and **never touch** the other
 * flags — the analysis pipeline and the stance materializer own those independently.
 */
class DoobieStanceMaterializationStatusRepository extends StanceMaterializationStatusRepository {

  /**
   * Upsert that sets `has_votes = TRUE` and refreshes `votes_updated_at` to the server `NOW()`. `NOW()` runs inside the
   * transaction and is derived from the connection clock, so every row written in a single transaction gets the same
   * timestamp — useful for correlating multi-step pipeline output.
   */
  override def markHasVotes(billId: Long): ConnectionIO[Unit] = {
    val table = Fragment.const(Tables.StanceMaterializationStatus)
    sql"""INSERT INTO $table (bill_id, has_votes, votes_updated_at)
          VALUES ($billId, TRUE, NOW())
          ON CONFLICT (bill_id) DO UPDATE SET
            has_votes = TRUE,
            votes_updated_at = NOW()""".update.run.void
  }

}
