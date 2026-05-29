package repcheck.members.committees.pipeline

import cats.effect.Async
import cats.syntax.all._

import fs2.Stream

import doobie.implicits._
import doobie.util.transactor.Transactor

import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}
import repcheck.members.committees.client.HistoricalAssignmentTsvReader
import repcheck.members.committees.config.HistoricalLoaderConfig
import repcheck.members.committees.errors.CommitteeMemberUpsertFailed
import repcheck.members.committees.model.{
  CommitteeInsert,
  CommitteeMemberInsert,
  HistoricalAssignmentRow,
  HistoricalLoadResult,
}
import repcheck.members.committees.persistence.{CommitteeMemberRepository, CommitteeRepository}
import repcheck.members.common.persistence.MemberRepository

/**
 * One-time loader for historical committee membership. Reads the canonical TSV (one row per member/committee/congress),
 * resolves each member by bioguide_id, maps the committee code to an existing `committees` row (creating it when the
 * source references a committee Phase 1's Congress.gov metadata never had), and upserts `committee_members` under the
 * row's OWN congress — unlike the live refresher, which writes everything under the current congress.
 *
 * Member resolution is bioguide-only: assignments for members not yet in the `members` table are skipped and counted,
 * not guessed. Backfill member profiles for the target congresses first.
 */
class CommitteeHistoryLoader[F[_]: Async](
  committeeRepo: CommitteeRepository,
  committeeMemberRepo: CommitteeMemberRepository,
  memberRepo: MemberRepository,
  xa: Transactor[F],
  config: HistoricalLoaderConfig,
  logger: PipelineLogger[F],
) {

  private val StepName = "committee-history-loader"

  def load(lines: Stream[F, String], runId: Long): F[HistoricalLoadResult] = {
    val logCtx = LogContext(runId = runId.toString, stepName = StepName)
    lines
      .filter(line => line.trim.nonEmpty && !HistoricalAssignmentTsvReader.isHeader(line))
      .parEvalMap(config.parallelism)(line => processLine(line, logCtx))
      .compile
      .fold(HistoricalLoadResult.empty)(_.combine(_))
  }

  private def processLine(line: String, logCtx: LogContext): F[HistoricalLoadResult] =
    HistoricalAssignmentTsvReader.parseLine(line) match {
      case Left(err) =>
        logger
          .warn(logCtx, s"Skipping malformed row: $err")
          .as(HistoricalLoadResult(rowsRead = 1, upserted = 0, skippedNoMember = 0, parseErrors = 1))
      case Right(row) =>
        upsertAssignment(row, logCtx)
    }

  private def upsertAssignment(row: HistoricalAssignmentRow, logCtx: LogContext): F[HistoricalLoadResult] =
    memberRepo.findByBioguideId(row.bioguideId).transact(xa).flatMap {
      case None =>
        logger
          .debug(
            logCtx,
            s"Skipping ${row.bioguideId} / ${row.committeeCode} / congress ${row.congress.toString} — " +
              "member not in members table",
          )
          .as(HistoricalLoadResult(rowsRead = 1, upserted = 0, skippedNoMember = 1, parseErrors = 0))
      case Some(member) =>
        for {
          committeeId <- resolveCommitteeId(row)
          insert = CommitteeMemberInsert(
            committeeId = committeeId,
            memberId = member.memberId,
            role = CommitteeMemberInsert.normalizeRole(row.role),
            side = None,
            rank = row.rank,
            congress = row.congress,
          )
          _ <- committeeMemberRepo.upsert(insert).transact(xa).handleErrorWith { error =>
            Async[F].raiseError(
              CommitteeMemberUpsertFailed(
                row.committeeCode,
                member.memberId,
                Option(error.getMessage).getOrElse("unknown"),
                Some(error),
              )
            )
          }
        } yield HistoricalLoadResult(rowsRead = 1, upserted = 1, skippedNoMember = 0, parseErrors = 0)
    }

  /**
   * Map the source committee code to a `committees.id`. Prefers an existing row (preserving Phase 1's Congress.gov
   * metadata); otherwise creates one from the TSV's name/type, or a bare placeholder when no name is supplied.
   */
  private def resolveCommitteeId(row: HistoricalAssignmentRow): F[Long] =
    committeeRepo.findByCode(row.committeeCode).transact(xa).flatMap {
      case Some(existing) => Async[F].pure(existing.id)
      case None =>
        val create = row.committeeName match {
          case Some(name) =>
            committeeRepo.upsert(
              CommitteeInsert(
                naturalKey = row.committeeCode,
                name = name,
                chamber = row.chamber,
                committeeType = row.committeeType,
                parentCommitteeId = None,
                url = None,
                updateDate = None,
                isCurrent = None,
              )
            )
          case None =>
            committeeRepo.upsertPlaceholder(row.committeeCode, row.chamber)
        }
        create.transact(xa).map(_.id)
    }

}
