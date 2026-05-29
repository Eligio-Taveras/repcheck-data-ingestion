package repcheck.members.committees.pipeline

import cats.effect.{Async, Ref}
import cats.syntax.all._

import doobie.implicits._
import doobie.util.transactor.Transactor

import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}
import repcheck.members.committees.client.{SenateCommitteeMembershipXmlClient, SenateIdentityXmlClient}
import repcheck.members.committees.config.CommitteeMembershipConfig
import repcheck.members.committees.errors.CommitteeMemberUpsertFailed
import repcheck.members.committees.model.{CommitteeMemberInsert, SenateCommitteeMemberXmlDTO, SenatorIdentityXmlDTO}
import repcheck.members.committees.persistence.{CommitteeMemberRepository, CommitteeRepository}
import repcheck.members.common.persistence.MemberRepository

private[pipeline] class Phase3SenateProcessor[F[_]: Async](
  senateIdentityClient: SenateIdentityXmlClient[F],
  senateCommitteeClient: SenateCommitteeMembershipXmlClient[F],
  committeeRepo: CommitteeRepository,
  committeeMemberRepo: CommitteeMemberRepository,
  memberRepo: MemberRepository,
  xa: Transactor[F],
  config: CommitteeMembershipConfig,
  logger: PipelineLogger[F],
) {

  def process(runId: Long, logCtx: LogContext): F[Unit] =
    for {
      _           <- logger.info(logCtx, "Phase 3: Processing Senate committee membership")
      identities  <- senateIdentityClient.fetchIdentities(runId).compile.toList
      identityMap <- buildSenateIdentityMap(identities, logCtx)
      // The set of committees that actually exist this Congress is the distinct set referenced by
      // current senators in cvc_member_data.xml — not the DB's committee metadata, which retains
      // defunct committees and omits the joint committees senators sit on.
      committeeCodes = identities.flatMap(_.committeeCodes).distinct.sorted
      _ <- logger.info(logCtx, s"Found ${committeeCodes.size.toString} current Senate committees to process")
      _ <- committeeCodes.traverse_ { code =>
        senateCommitteeClient
          .fetchCommitteeMembers(code, runId)
          .evalMap(senMember => resolveSenateCommitteeMember(senMember, identityMap, logCtx))
          .compile
          .drain
      }
      _ <- logger.info(logCtx, "Phase 3 complete")
    } yield ()

  private[pipeline] def buildSenateIdentityMap(
    identities: List[SenatorIdentityXmlDTO],
    logCtx: LogContext,
  ): F[Map[(String, String, String), (String, Long)]] =
    for {
      ref <- Ref.of[F, Map[(String, String, String), (String, Long)]](Map.empty)
      _ <- identities.traverse_ { identity =>
        memberRepo.findByBioguideId(identity.bioguideId).transact(xa).flatMap {
          case Some(member) =>
            val key = (identity.firstName.toLowerCase, identity.lastName.toLowerCase, identity.state.toUpperCase)
            ref.update(_.updated(key, (identity.bioguideId, member.memberId)))
          case None =>
            logger.debug(
              logCtx,
              s"Senate identity ${identity.bioguideId} not in members table, skipping identity map entry",
            )
        }
      }
      result <- ref.get
      _      <- logger.info(logCtx, s"Built Senate identity map with ${result.size.toString} entries")
    } yield result

  private def resolveSenateCommitteeMember(
    senMember: SenateCommitteeMemberXmlDTO,
    identityMap: Map[(String, String, String), (String, Long)],
    logCtx: LogContext,
  ): F[Unit] = {
    val resolvedMemberId: F[Option[Long]] = senMember.bioguideId match {
      case Some(bio) =>
        memberRepo.findByBioguideId(bio).transact(xa).map(_.map(_.memberId))
      case None =>
        val key = (senMember.firstName.toLowerCase, senMember.lastName.toLowerCase, senMember.state.toUpperCase)
        Async[F].pure(identityMap.get(key).map(_._2))
    }

    for {
      maybeMemberId <- resolvedMemberId
      _ <- maybeMemberId match {
        case Some(memberId) =>
          for {
            committee <- committeeRepo.upsertPlaceholder(senMember.committeeCode, "Senate").transact(xa)
            insert = CommitteeMemberInsert(
              committeeId = committee.id,
              memberId = memberId,
              role = CommitteeMemberInsert.normalizeRole(senMember.position),
              side = None,
              rank = senMember.rank,
              congress = config.currentCongress,
            )
            _ <- committeeMemberRepo.upsert(insert).transact(xa).handleErrorWith { error =>
              Async[F].raiseError(
                CommitteeMemberUpsertFailed(
                  senMember.committeeCode,
                  memberId,
                  Option(error.getMessage).getOrElse("unknown"),
                  Some(error),
                )
              )
            }
          } yield ()
        case None =>
          logger.debug(
            logCtx,
            s"Could not resolve Senate member ${senMember.firstName} ${senMember.lastName} " +
              s"(${senMember.state}) for committee ${senMember.committeeCode}",
          )
      }
    } yield ()
  }

}
