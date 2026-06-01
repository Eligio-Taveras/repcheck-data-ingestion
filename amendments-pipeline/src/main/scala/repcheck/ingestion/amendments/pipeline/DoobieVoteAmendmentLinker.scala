package repcheck.ingestion.amendments.pipeline

import cats.effect.Async
import cats.syntax.all._

import doobie._
import doobie.implicits._
import doobie.util.transactor.Transactor

import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}
import repcheck.pipeline.models.constants.Tables

/**
 * Doobie implementation of [[VoteAmendmentLinker]]. One idempotent reconciliation UPDATE per pass.
 *
 * The CTE parses the roll-call number out of `amendments.latest_action_text` (a different sentence shape per chamber),
 * joins to `votes` on `(congress, chamber, roll_number)`, and a window `count(*) OVER (PARTITION BY vote)` flags
 * en-bloc collisions so the final UPDATE can require `n = 1`. Matched votes get `amendment_id` plus the natural-key
 * columns (`legislation_type = AMENDMENT`, `amendment_type`, `legislation_number`); `bill_type`/`bill_id` are cleared
 * because the `votes_legislation_type_subtype_check` constraint forbids a bill subtype on an AMENDMENT row (the parent
 * bill stays reachable via `amendment_id → amendments.bill_id`). The `amendment_id IS DISTINCT FROM` guard keeps the
 * statement a no-op once everything is linked.
 *
 * `amendment_type` discriminates the chamber: `hamdt` → House, `samdt`/`suamdt` → Senate.
 */
class DoobieVoteAmendmentLinker[F[_]: Async](
  xa: Transactor[F],
  logger: PipelineLogger[F],
) extends VoteAmendmentLinker[F] {

  private val votes: Fragment      = Fragment.const(Tables.Votes)
  private val amendments: Fragment = Fragment.const(Tables.Amendments)

  private val linkStatement: Fragment =
    fr"""
      WITH amd AS (
        SELECT id AS amendment_id, congress, amendment_type, number,
          CASE
            WHEN amendment_type = 'hamdt'
              THEN (regexp_match(latest_action_text, '[Rr]oll *(?:no|call)\.? *([0-9]+)'))[1]
            ELSE (regexp_match(latest_action_text, '[Rr]ecord [Vv]ote (?:[Nn]umber|[Nn]o)\.?:? *([0-9]+)'))[1]
          END AS roll
        FROM """ ++ amendments ++ fr"""
        WHERE latest_action_text IS NOT NULL
          AND amendment_type IN ('hamdt', 'samdt', 'suamdt')
      ),
      matched AS (
        SELECT v.id AS vote_id, amd.amendment_id, amd.amendment_type, amd.number,
               count(*) OVER (PARTITION BY v.id) AS n
        FROM amd
        JOIN """ ++ votes ++ fr""" v
          ON v.congress = amd.congress
         AND v.chamber = (CASE WHEN amd.amendment_type = 'hamdt' THEN 'House' ELSE 'Senate' END)::chamber_type
         AND v.roll_number = amd.roll::int
        WHERE amd.roll IS NOT NULL
      )
      UPDATE """ ++ votes ++ fr""" v SET
        amendment_id       = matched.amendment_id,
        legislation_type   = 'AMENDMENT',
        amendment_type     = matched.amendment_type,
        legislation_number = matched.number,
        bill_type          = NULL,
        bill_id            = NULL,
        updated_at         = NOW()
      FROM matched
      WHERE v.id = matched.vote_id
        AND matched.n = 1
        AND v.amendment_id IS DISTINCT FROM matched.amendment_id
    """

  override def linkAll(ctx: LogContext): F[Int] =
    for {
      updated <- linkStatement.update.run.transact(xa)
      _       <- logger.info(ctx, s"vote-amendment-linker: linked $updated vote(s) to amendments this pass")
    } yield updated

}
