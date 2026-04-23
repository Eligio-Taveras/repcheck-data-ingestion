package repcheck.ingestion.votes.pipeline

import cats.effect.Async
import cats.syntax.all._

import doobie.implicits._
import doobie.util.transactor.Transactor

import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}
import repcheck.ingestion.common.placeholders.{EntityRepository, PlaceholderCreator}
import repcheck.ingestion.votes.errors.MemberResolutionFailed
import repcheck.members.common.persistence.MemberRepository
import repcheck.shared.models.congress.dos.member.MemberDO
import repcheck.shared.models.congress.dos.results.UnresolvedVotePosition

/**
 * Resolves House bioguide ids (e.g. `"A000055"`) to `members.id` longs, creating a placeholder member row first if the
 * member isn't already persisted. Mirrors [[BillLookup]] but for bioguides — the members-pipeline enriches placeholders
 * on its next run.
 *
 * Two entry points:
 *   - [[resolveBioguide]] handles a single bioguide. The transaction boundary is the placeholder insert + FK lookup;
 *     the member id it returns is bound later by the persister's transaction when the position rows are written.
 *   - [[resolveAll]] deduplicates the bioguide list and traverses each through [[resolveBioguide]], returning a
 *     `bioguide → memberId` map suitable for driving [[VotePositionBuilders.house]].
 *
 * Senate positions do not come through here — Senate LIS ids are handled by
 * [[repcheck.ingestion.votes.lis.LisResolver]] which is a separate component with its own placeholder-merge semantics.
 */
private[pipeline] class MemberLookup[F[_]: Async](
  memberRepo: MemberRepository,
  memberEntityRepo: EntityRepository[F, MemberDO],
  placeholderCreator: PlaceholderCreator[F],
  xa: Transactor[F],
  logger: PipelineLogger[F],
) {

  /**
   * Resolve every House bioguide from the conversion result to a `members.id`. Positions with `memberSource = Right(_)`
   * or empty bioguides are filtered out defensively.
   */
  def resolveAll(positions: List[UnresolvedVotePosition], logCtx: LogContext): F[Map[String, Long]] = {
    val distinctBioguides = positions.iterator
      .flatMap(_.memberSource.left.toOption)
      .filter(_.nonEmpty)
      .toList
      .distinct

    distinctBioguides.traverse(bid => resolveBioguide(bid, logCtx).map(id => bid -> id)).map(_.toMap)
  }

  /**
   * Resolve a single bioguide. Raises [[MemberResolutionFailed]] on the pathological "placeholder inserted then
   * immediately absent" path, same defensive check as [[BillLookup.forContext]].
   */
  def resolveBioguide(bioguideId: String, logCtx: LogContext): F[Long] =
    for {
      _       <- placeholderCreator.ensureExists[MemberDO](bioguideId, memberEntityRepo)
      maybeId <- memberRepo.findByBioguideId(bioguideId).map(_.map(_.memberId)).transact(xa)
      id <- maybeId match {
        case Some(id) => Async[F].pure(id)
        case None =>
          val err = MemberResolutionFailed(bioguideId, "findByBioguideId returned None after ensureExists")
          logger.error(logCtx, err.getMessage, Some(err)) *> Async[F].raiseError[Long](err)
      }
    } yield id

}
