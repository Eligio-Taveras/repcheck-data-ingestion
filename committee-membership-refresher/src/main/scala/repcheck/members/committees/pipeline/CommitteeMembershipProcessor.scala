package repcheck.members.committees.pipeline

import cats.effect.Async
import cats.syntax.all._

import doobie.implicits._
import doobie.util.transactor.Transactor

import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}
import repcheck.members.committees.client.{
  CommitteeMetadataApiClient,
  HouseMemberDataXmlClient,
  SenateCommitteeMembershipXmlClient,
  SenateIdentityXmlClient,
}
import repcheck.members.committees.config.CommitteeMembershipConfig
import repcheck.members.committees.persistence.{CommitteeMemberRepository, CommitteeRepository}
import repcheck.members.common.persistence.MemberRepository

class CommitteeMembershipProcessor[F[_]: Async](
  committeeApiClient: CommitteeMetadataApiClient[F],
  houseXmlClient: HouseMemberDataXmlClient[F],
  senateIdentityClient: SenateIdentityXmlClient[F],
  senateCommitteeClient: SenateCommitteeMembershipXmlClient[F],
  committeeRepo: CommitteeRepository,
  committeeMemberRepo: CommitteeMemberRepository,
  memberRepo: MemberRepository,
  xa: Transactor[F],
  config: CommitteeMembershipConfig,
  logger: PipelineLogger[F],
) {

  private val StepName: String = "committee-membership-refresh"

  private val phase1 = new Phase1CommitteeMetadataFetcher[F](
    committeeApiClient,
    committeeRepo,
    xa,
    config,
    logger,
  )

  private val phase2 = new Phase2HouseProcessor[F](
    houseXmlClient,
    committeeRepo,
    committeeMemberRepo,
    memberRepo,
    xa,
    config,
    logger,
  )

  private val phase3 = new Phase3SenateProcessor[F](
    senateIdentityClient,
    senateCommitteeClient,
    committeeRepo,
    committeeMemberRepo,
    memberRepo,
    xa,
    config,
    logger,
  )

  def refreshAll(runId: Long): F[CommitteeMembershipRefreshResult] = {
    val logCtx = LogContext(runId = runId.toString, stepName = StepName)

    for {
      _                    <- logger.info(logCtx, "Starting committee membership refresh")
      _                    <- phase1.process(runId, logCtx)
      _                    <- phase2.process(runId, logCtx)
      _                    <- phase3.process(runId, logCtx)
      committeesCount      <- committeeRepo.countCurrent().transact(xa)
      membershipRowsCount  <- committeeMemberRepo.countByCongress(config.currentCongress).transact(xa)
      distinctMembersCount <- committeeMemberRepo.countDistinctMembersByCongress(config.currentCongress).transact(xa)
      result = CommitteeMembershipRefreshResult(
        committeesUpserted = committeesCount,
        distinctMembersProcessed = distinctMembersCount,
        membershipRowsUpserted = membershipRowsCount,
      )
      _ <- logger.info(
        logCtx,
        s"Committee membership refresh complete: committees=${result.committeesUpserted.toString} " +
          s"distinctMembers=${result.distinctMembersProcessed.toString} " +
          s"membershipRows=${result.membershipRowsUpserted.toString}",
      )
    } yield result
  }

}
