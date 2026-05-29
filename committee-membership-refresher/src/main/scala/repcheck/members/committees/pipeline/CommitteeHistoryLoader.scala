package repcheck.members.committees.pipeline

import cats.effect.Async
import cats.syntax.all._

import doobie.implicits._
import doobie.util.transactor.Transactor

import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}
import repcheck.members.committees.client.{CdirCommitteeListingParser, CdirCommitteeSource}
import repcheck.members.committees.config.HistoricalLoaderConfig
import repcheck.members.committees.errors.CommitteeMemberUpsertFailed
import repcheck.members.committees.model.{
  CdirAssignment,
  CommitteeMemberInsert,
  HistoricalLoadResult,
  HistoricalMemberRow,
}
import repcheck.members.committees.persistence.{
  CommitteeMemberRepository,
  CommitteeRepository,
  HistoricalMemberRepository,
}

/**
 * Backfills historical committee membership from the GovInfo Congressional Directory. The committee universe is driven
 * off the DB (per the design): we enumerate `committees`, use their names to anchor the CDIR parser, and map parsed
 * assignments back to committee ids by normalized name. Members are resolved by (last name, state) within the target
 * congress, disambiguated by first name; the row's OWN congress is written. Unresolved committees/members are counted,
 * not guessed.
 */
class CommitteeHistoryLoader[F[_]: Async](
  source: CdirCommitteeSource[F],
  committeeRepo: CommitteeRepository,
  committeeMemberRepo: CommitteeMemberRepository,
  memberRepo: HistoricalMemberRepository,
  xa: Transactor[F],
  config: HistoricalLoaderConfig,
  stateNames: Set[String],
  logger: PipelineLogger[F],
) {

  private val StepName = "committee-history-loader"

  private type MemberIndex = Map[(String, String), List[(String, Long)]]

  def load(runId: Long): F[HistoricalLoadResult] = {
    val logCtx = LogContext(runId = runId.toString, stepName = StepName)
    for {
      committees <- committeeRepo.listAll().transact(xa)
      knownNames     = committees.map(_.name).toSet
      committeeIndex = committees.map(c => CdirCommitteeListingParser.normalizeCommittee(c.name) -> c.id).toMap
      _ <- logger.info(
        logCtx,
        s"Loaded ${committees.size.toString} committees; backfilling congresses " +
          config.targetCongresses.mkString(", "),
      )
      result <- config.targetCongresses.foldLeftM(HistoricalLoadResult.empty) { (acc, congress) =>
        loadCongress(congress, knownNames, committeeIndex, runId).map(acc.combine)
      }
      _ <- logger.info(
        logCtx,
        s"Historical load complete: seen=${result.assignmentsSeen.toString} upserted=${result.upserted.toString} " +
          s"noMember=${result.skippedNoMember.toString} noCommittee=${result.skippedNoCommittee.toString}",
      )
    } yield result
  }

  private def loadCongress(
    congress: Int,
    knownNames: Set[String],
    committeeIndex: Map[String, Long],
    runId: Long,
  ): F[HistoricalLoadResult] =
    for {
      members <- memberRepo.membersForCongress(congress).transact(xa)
      memberIndex = buildMemberIndex(members)
      texts <- source.committeeListingTexts(congress, runId)
      assignments = texts.flatMap(t => CdirCommitteeListingParser.parse(t, knownNames, stateNames))
      result <- assignments.foldLeftM(HistoricalLoadResult.empty) { (acc, a) =>
        upsertAssignment(a, congress, committeeIndex, memberIndex).map(acc.combine)
      }
    } yield result

  private def upsertAssignment(
    a: CdirAssignment,
    congress: Int,
    committeeIndex: Map[String, Long],
    memberIndex: MemberIndex,
  ): F[HistoricalLoadResult] = {
    val committeeId = committeeIndex.get(CdirCommitteeListingParser.normalizeCommittee(a.committeeName))
    val memberId    = resolveMember(a, memberIndex)
    (committeeId, memberId) match {
      case (None, _) =>
        Async[F].pure(HistoricalLoadResult.single(upserted = false, noMember = false, noCommittee = true))
      case (_, None) =>
        Async[F].pure(HistoricalLoadResult.single(upserted = false, noMember = true, noCommittee = false))
      case (Some(cid), Some(mid)) =>
        val insert = CommitteeMemberInsert(
          committeeId = cid,
          memberId = mid,
          role = CommitteeMemberInsert.normalizeRole(a.role),
          side = None,
          rank = None,
          congress = congress,
        )
        committeeMemberRepo
          .upsert(insert)
          .transact(xa)
          .handleErrorWith { error =>
            Async[F].raiseError(
              CommitteeMemberUpsertFailed(
                a.committeeName,
                mid,
                Option(error.getMessage).getOrElse("unknown"),
                Some(error),
              )
            )
          }
          .as(HistoricalLoadResult.single(upserted = true, noMember = false, noCommittee = false))
    }
  }

  private def buildMemberIndex(members: List[HistoricalMemberRow]): MemberIndex =
    members
      .flatMap { m =>
        for {
          last  <- m.lastName
          state <- m.stateName
        } yield (norm(last), norm(state)) -> (m.firstName.map(norm).getOrElse(""), m.memberId)
      }
      .groupMap(_._1)(_._2)

  private def resolveMember(a: CdirAssignment, memberIndex: MemberIndex): Option[Long] = {
    val first = norm(a.firstName)
    memberIndex.get((norm(a.lastName), norm(a.state))).flatMap {
      case (_, id) :: Nil => Some(id)
      case candidates =>
        candidates
          .find { case (f, _) => f == first }
          .orElse(candidates.find { case (f, _) => first.nonEmpty && f.startsWith(first.substring(0, 1)) })
          .map { case (_, id) => id }
    }
  }

  private def norm(s: String): String = s.toLowerCase.trim

}
