package repcheck.ingestion.votes.repo

import cats.syntax.all._

import doobie._
import doobie.implicits._

import repcheck.pipeline.models.constants.Tables

/**
 * Doobie implementation of [[StanceMaterializationStatusRepository]]. The only operation the votes pipeline performs on
 * `stance_materialization_status` is `markHasVotes`: an INSERT with ON CONFLICT DO UPDATE keyed by the PK `bill_id`. We
 * explicitly do NOT touch `has_analysis`, `all_passes_completed`, or any of the *_completed_at columns — those are
 * owned by the analysis pipeline and the stance materializer respectively.
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
