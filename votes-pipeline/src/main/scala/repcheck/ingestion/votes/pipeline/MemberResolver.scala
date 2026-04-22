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

/**
 * Resolves House `bioguideID` strings to internal `members.id` Long PKs for vote-position writes.
 *
 * ==Flow==
 *   1. `PlaceholderCreator.ensureExists[MemberDO]` performs an idempotent insert-if-not-exists on `members` keyed by
 *      the bioguide natural key. No-op when the row already exists; otherwise writes a stub `MemberDO` whose
 *      placeholder fields are overwritten by the next members-pipeline run.
 *   2. `MemberRepository.findByBioguideId(bioguide)` reads back the row's surrogate `members.id`. Both steps are in
 *      `F[_]`, each running its own short transaction via `.transact(xa)` — there is no need to compose them under one
 *      boundary because the insert is idempotent and the subsequent read cannot observe a torn state.
 *
 * If `findByBioguideId` returns `None` after `ensureExists` succeeds, the resolver raises [[MemberResolutionFailed]]
 * rather than silently producing a missing identity. That scenario is pathological (would require another actor to
 * delete the row between the two calls) but we surface it as a per-vote failure so the stream keeps processing other
 * votes.
 *
 * Senate positions do NOT go through this resolver — they carry `lis_member_id` and bypass the `members` table per the
 * migration 023 dual-identity design. Only House positions are translated here.
 */
private[pipeline] class MemberResolver[F[_]: Async](
  memberRepo: MemberRepository,
  placeholderCreator: PlaceholderCreator[F],
  memberEntityRepo: EntityRepository[F, MemberDO],
  xa: Transactor[F],
  logger: PipelineLogger[F],
) {

  /**
   * Resolve a single bioguide id to an internal `members.id`. Creates a placeholder first, then looks up. Raises
   * [[MemberResolutionFailed]] if the lookup returns `None` after the placeholder write succeeded.
   */
  def resolveBioguide(bioguideId: String, logCtx: LogContext): F[Long] =
    for {
      _         <- placeholderCreator.ensureExists[MemberDO](bioguideId, memberEntityRepo)
      maybeRow  <- memberRepo.findByBioguideId(bioguideId).transact(xa)
      memberRow <- maybeRow match {
        case Some(m) => Async[F].pure(m)
        case None =>
          val err = MemberResolutionFailed(
            bioguideId = bioguideId,
            detail = "findByBioguideId returned None after ensureExists — placeholder row disappeared",
          )
          logger.error(logCtx, err.getMessage, Some(err)) *> Async[F].raiseError[MemberDO](err)
      }
    } yield memberRow.memberId

  /**
   * Batch version. Resolves each distinct bioguide at most once and returns a `Map[bioguide, memberId]` for the caller
   * to thread into position DOs. Deduplication is on the caller side's responsibility (the processor dedupes via the
   * DTO-level position list), but the resolver also defensively distincts the input to avoid redundant DB round-trips
   * on a duplicated feed.
   */
  def resolveBatch(bioguideIds: List[String], logCtx: LogContext): F[Map[String, Long]] =
    bioguideIds.distinct
      .traverse(bid => resolveBioguide(bid, logCtx).map(id => bid -> id))
      .map(_.toMap)

}
