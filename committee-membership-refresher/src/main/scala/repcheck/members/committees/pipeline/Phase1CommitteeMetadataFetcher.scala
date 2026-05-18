package repcheck.members.committees.pipeline

import java.time.Instant

import cats.effect.Async
import cats.syntax.all._

import doobie.implicits._
import doobie.util.transactor.Transactor

import repcheck.ingestion.common.api.FetchParams
import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}
import repcheck.members.committees.client.CommitteeMetadataApiClient
import repcheck.members.committees.config.CommitteeMembershipConfig
import repcheck.members.committees.errors.CommitteeUpsertFailed
import repcheck.members.committees.model.{CommitteeCodeNormalizer, CommitteeInsert}
import repcheck.members.committees.persistence.CommitteeRepository

private[pipeline] class Phase1CommitteeMetadataFetcher[F[_]: Async](
  committeeApiClient: CommitteeMetadataApiClient[F],
  committeeRepo: CommitteeRepository,
  xa: Transactor[F],
  config: CommitteeMembershipConfig,
  logger: PipelineLogger[F],
) {

  def process(runId: Long, logCtx: LogContext): F[Unit] = {
    val _ = runId
    for {
      _ <- logger.info(logCtx, "Phase 1: Fetching committee metadata from Congress.gov")
      _ <- fetchAndUpsertChamberCommittees("house", logCtx)
      _ <- fetchAndUpsertChamberCommittees("senate", logCtx)
      _ <- fetchAndUpsertChamberCommittees("joint", logCtx)
      _ <- logger.info(logCtx, "Phase 1 complete")
    } yield ()
  }

  private def fetchAndUpsertChamberCommittees(
    chamber: String,
    logCtx: LogContext,
  ): F[Unit] = {
    def fetchPage(offset: Int): F[Unit] = {
      val params = FetchParams(offset = offset, pageSize = config.pageSize, congress = None)
      for {
        _    <- logger.debug(logCtx, s"Fetching $chamber committees page offset=$offset")
        page <- committeeApiClient.fetchPageForChamber(chamber, params)
        _ <- page.items.traverse_ { item =>
          val committeeCode = CommitteeCodeNormalizer.fromCongressGov(item.systemCode, chamber)
          val parentCode =
            item.parent.flatMap(_.systemCode).map(pc => CommitteeCodeNormalizer.fromCongressGov(pc, chamber))
          val committeeType = item.committeeTypeCode.getOrElse(
            if (parentCode.isDefined) "Subcommittee" else "Standing"
          )

          val resolveParentId: F[Option[Long]] = parentCode match {
            case Some(pc) =>
              committeeRepo.findByCode(pc).transact(xa).map(_.map(_.id))
            case None => Async[F].pure(None)
          }

          for {
            parentId <- resolveParentId
            insert = CommitteeInsert(
              naturalKey = committeeCode,
              name = item.name,
              chamber = chamber.capitalize,
              committeeType = Some(committeeType),
              parentCommitteeId = parentId,
              url = item.url,
              updateDate = item.updateDate.map(d => Instant.parse(s"${d}T00:00:00Z")),
              isCurrent = Some(true),
            )
            _ <- committeeRepo
              .upsert(insert)
              .transact(xa)
              .handleErrorWith { error =>
                Async[F].raiseError(
                  CommitteeUpsertFailed(committeeCode, Option(error.getMessage).getOrElse("unknown"), Some(error))
                )
              }
          } yield ()
        }
        _ <- page.nextOffset match {
          case Some(next) => fetchPage(next)
          case None       => Async[F].unit
        }
      } yield ()
    }

    fetchPage(0)
  }

}
