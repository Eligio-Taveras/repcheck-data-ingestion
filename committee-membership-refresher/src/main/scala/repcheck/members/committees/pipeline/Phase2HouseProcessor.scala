package repcheck.members.committees.pipeline

import cats.effect.Async
import cats.syntax.all._

import doobie.implicits._
import doobie.util.transactor.Transactor

import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}
import repcheck.members.committees.client.HouseMemberDataXmlClient
import repcheck.members.committees.config.CommitteeMembershipConfig
import repcheck.members.committees.errors.CommitteeMemberUpsertFailed
import repcheck.members.committees.model.{CommitteeMemberInsert, HouseMemberDataXmlDTO}
import repcheck.members.committees.persistence.{CommitteeMemberRepository, CommitteeRepository}
import repcheck.members.common.persistence.MemberRepository

private[pipeline] class Phase2HouseProcessor[F[_]: Async](
  houseXmlClient: HouseMemberDataXmlClient[F],
  committeeRepo: CommitteeRepository,
  committeeMemberRepo: CommitteeMemberRepository,
  memberRepo: MemberRepository,
  xa: Transactor[F],
  config: CommitteeMembershipConfig,
  logger: PipelineLogger[F],
) {

  def process(runId: Long, logCtx: LogContext): F[Unit] =
    for {
      _ <- logger.info(logCtx, "Phase 2: Processing House member committee data")
      _ <- houseXmlClient
        .fetchMembers(runId)
        .evalMap(dto => resolveAndUpsertHouseMember(dto, logCtx))
        .compile
        .drain
      _ <- logger.info(logCtx, "Phase 2 complete")
    } yield ()

  private def resolveAndUpsertHouseMember(
    dto: HouseMemberDataXmlDTO,
    logCtx: LogContext,
  ): F[Unit] = {
    val itemCtx = logCtx.copy(entityId = Some(dto.bioguideId))

    for {
      maybeMember <- memberRepo.findByBioguideId(dto.bioguideId).transact(xa)
      _ <- maybeMember match {
        case Some(member) =>
          dto.committees.traverse_ { assignment =>
            for {
              committee <- committeeRepo.upsertPlaceholder(assignment.committeeCode, "House").transact(xa)
              insert = CommitteeMemberInsert(
                committeeId = committee.id,
                memberId = member.memberId,
                role = None,
                side = assignment.side,
                rank = assignment.rank,
                congress = config.currentCongress,
              )
              _ <- committeeMemberRepo.upsert(insert).transact(xa).handleErrorWith { error =>
                Async[F].raiseError(
                  CommitteeMemberUpsertFailed(
                    assignment.committeeCode,
                    member.memberId,
                    Option(error.getMessage).getOrElse("unknown"),
                    Some(error),
                  )
                )
              }
            } yield ()
          }
        case None =>
          logger.debug(itemCtx, s"Skipping House member ${dto.bioguideId} — not in members table")
      }
    } yield ()
  }

}
